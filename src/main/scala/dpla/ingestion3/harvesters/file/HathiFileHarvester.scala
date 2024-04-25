package dpla.ingestion3.harvesters.file

import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream
import dpla.ingestion3.confs.i3Conf
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.tools.tar.TarInputStream
import dpla.ingestion3.harvesters.file.FileFilters.GzFileFilter
import dpla.ingestion3.mappers.utils.XmlExtractor
import dpla.ingestion3.model.AVRO_MIME_XML
import org.apache.avro.generic.GenericData
import org.apache.logging.log4j.LogManager

import scala.util.{Failure, Success, Try}
import scala.xml._

class HathiFileHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends FileHarvester(spark, shortName, conf)
    with XmlExtractor {

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML

  /** Loads .tar.gz files
    *
    * @param file
    *   File to parse
    * @return
    *   Option[TarInputStream] of the zip contents
    */
  def getInputStream(file: File): Option[TarInputStream] = {
    file.getName match {
      case zipName if zipName.endsWith("gz") =>
        Some(new TarInputStream(new GZIPInputStream(new FileInputStream(file))))
      case zipName if zipName.endsWith("tar") =>
        Some(new TarInputStream(new FileInputStream(file)))

      case _ => None
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
    val logger = LogManager.getLogger(this.getClass)
    for {
      items <- xml \\ "record" :: Nil
      item <- items
    } yield item match {
      case record: Node =>
        // Extract required record identifier
        val id: Option[String] = (record \ "controlfield")
          .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "tag", "001"))
          .map(extractString)
          .headOption
          .flatten

        val outputXML = xmlToString(record)

        id match {
          case None =>
            logger.warn(s"Missing required record_ID for $outputXML")
            None
          case Some(id) => Some(ParsedResult(id, outputXML))
        }
      case _ =>
        logger.warn("Got weird result back for item path: " + item.getClass)
        None
    }
  }

  /** Implements a stream of files from the tar. Can't use @tailrec here because
    * the compiler can't recognize it as tail recursive, but this won't blow the
    * stack.
    *
    * @param tarInputStream
    * @return
    *   Lazy stream of tar records
    */
  def iter(tarInputStream: TarInputStream): Stream[FileResult] =
    Option(tarInputStream.getNextEntry) match {
      case None =>
        Stream.empty

      case Some(entry) =>
        val filename = Try {
          entry.getName
        }.getOrElse("")

        val result =
          if (
            entry.isDirectory || filename.contains("._")
          ) // drop OSX hidden files
            None
          else if (filename.endsWith(".xml")) // only read xml files
            Some(IOUtils.toByteArray(tarInputStream, entry.getSize))
          else
            None

        FileResult(entry.getName, result) #:: iter(tarInputStream)
    }

  /** Executes the harvest
    */
  override def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles
      .listFiles(new GzFileFilter)
      .foreach(inFile => {

        val inputStream = getInputStream(inFile)
          .getOrElse(
            throw new IllegalArgumentException(
              s"Couldn't load file, ${inFile.getAbsolutePath}"
            )
          )

        val recordCount = (for (tarResult <- iter(inputStream)) yield {
          handleFile(tarResult, unixEpoch) match {
            case Failure(exception) =>
              LogManager
                .getLogger(this.getClass)
                .error(
                  s"Caught exception on ${tarResult.entryName}.",
                  exception
                )
              0
            case Success(count) =>
              count
          }
        }).sum

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

  /** Main logic for handling individual entries in the tar.
    *
    * @param tarResult
    *   Case class representing extracted item from the tar
    * @return
    *   Count of metadata items found.
    */
  def handleFile(tarResult: FileResult, unixEpoch: Long): Try[Int] =
    tarResult.data match {
      case None =>
        Success(0) // a directory, no results

      case Some(data) =>
        Try {
          val dataString = new String(data).replaceAll("<\\?xml.*\\?>", "").trim

          val xml = XML.loadString(dataString)

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
}
