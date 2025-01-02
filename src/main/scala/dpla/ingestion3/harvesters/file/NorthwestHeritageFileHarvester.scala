package dpla.ingestion3.harvesters.file

import java.io.{File, FileInputStream}
import java.util.zip.ZipInputStream
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.file.FileFilters.zipFilter
import dpla.ingestion3.mappers.utils.XmlExtractor
import dpla.ingestion3.model.AVRO_MIME_XML
import org.apache.avro.generic.GenericData
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}
import scala.xml._

/** Extracts values from parsed XML
  */
class NorthwestHeritageFileExtractor extends XmlExtractor

/** Entry for harvesting MODS XML from the Northwest Heritage Hub
  */
class NorthwestHeritageFileHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends FileHarvester(spark, shortName, conf) {

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML

  protected val extractor = new OaiFileExtractor()

  /** Loads .zip files
    *
    * @param file
    *   File to parse
    * @return
    *   ZipInputstream of the zip contents
    *
    * TODO: Because we're only handling zips in this class, and they should
    * already be filtered by the FilenameFilter, I wonder if we even need the
    * match statement here.
    */
  def getInputStream(file: File): Option[ZipInputStream] = {
    file.getName match {
      case zipName if zipName.endsWith("zip") =>
        Some(new ZipInputStream(new FileInputStream(file)))
      case _ => None
    }
  }

  /** Main logic for handling individual entries in the zip.
    *
    * @param zipResult
    *   Case class representing extracted item from the zip
    * @return
    *   Count of metadata items found.
    */
  def handleFile(zipResult: FileResult, unixEpoch: Long): Try[Int] =
    zipResult.data match {
      case None =>
        Success(0) // a directory, no results

      case Some(data) =>
        Try {
          val xml = XML.loadString(new String(data)) // parse string to XML

          val items = handleXML(xml)

          val counts = for {
            itemOption <- items
            item <- itemOption // filters out the Nones
          } yield {
            writeOut(unixEpoch, item)
            1
          }
          counts.sum
        }
    }

  /** Takes care of parsing an xml file into a list of Nodes each representing
    * an item
    *
    * @param xml
    *   Root of the xml document
    * @return
    *   List of Options of id/item pairs.
    */
  def handleXML(xml: Node): List[Option[ParsedResult]] = {
    for {
      items <- xml \\ "mods" :: Nil
      item <- items
    } yield item match {
      case record: Node =>
        // Extract required record identifier
        val id: Option[String] = extractor.extractString(record \ "identifier")

        val outputXML = xmlToString(record)

        id match {
          case None =>
            LogManager
              .getLogger(this.getClass)
              .warn(s"Missing required record_ID for $outputXML")
            None
          case Some(id) => Some(ParsedResult(id, outputXML))
        }
      case _ =>
        LogManager
          .getLogger(this.getClass)
          .warn("Got weird result back for item path: " + item.getClass)
        None
    }
  }

  /** Implements a stream of files from the zip Can't use @tailrec here because
    * the compiler can't recognize it as tail recursive, but this won't blow the
    * stack.
    *
    * @param zipInputStream
    * @return
    *   Lazy stream of zip records
    */
  def iter(zipInputStream: ZipInputStream): LazyList[FileResult] =
    Option(zipInputStream.getNextEntry) match {
      case None =>
        LazyList.empty
      case Some(entry) =>
        val result =
          if (entry.isDirectory || !entry.getName.endsWith(".xml"))
            None
          else {
            Some(IOUtils.toByteArray(zipInputStream, entry.getSize))
          }
        FileResult(entry.getName, result) #:: iter(zipInputStream)
    }

  /** Executes the Northwest Heritage harvest
    */
  override def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles
      .listFiles(zipFilter)
      .foreach(inFile => {
        val inputStream = getInputStream(inFile)
          .getOrElse(
            throw new IllegalArgumentException("Couldn't load ZIP files.")
          )
        val recordCount = iter(inputStream).map(result =>
          handleFile(result, unixEpoch) match {
            case Failure(exception) =>
              LogManager
                .getLogger(this.getClass)
                .error(s"Caught exception on $inFile.", exception)
              0
            case Success(count) =>
              count
          }
        ).sum
        IOUtils.closeQuietly(inputStream)
      })

    // flush the avroWriter
    flush()

    // Read harvested data into Spark DataFrame and return.
    spark.read.format("avro").load(tmpOutStr)
  }

  /** Converts a Node to an xml string
    *
    * @param node
    *   The root of the tree to write to a string
    * @return
    *   a String containing xml
    */
  def xmlToString(node: Node): String =
    Utility.serialize(node, minimizeTags = MinimizeMode.Always).toString
}
