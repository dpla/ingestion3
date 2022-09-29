package dpla.ingestion3.harvesters.file

import java.io.{File, FileInputStream}
import java.util.zip.ZipInputStream
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.mappers.utils.XmlExtractor
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.spark.avro._
import dpla.ingestion3.harvesters.file.FileFilters.ZipFileFilter

import scala.util.{Failure, Success, Try}
import scala.xml.{MinimizeMode, Node, Utility, XML}


/**
  * Extracts values from parsed Xml
  */
class VaFileExtractor extends XmlExtractor

/**
  * Entry for performing a Digital Virginias file harvest
  */
class VaFileHarvester(spark: SparkSession,
                      shortName: String,
                      conf: i3Conf,
                      logger: Logger)
  extends FileHarvester(spark, shortName, conf, logger) {

  def mimeType: String = "application_xml"

  protected val extractor = new VaFileExtractor()

  /**
    * Loads .zip files
    *
    * @param file File to parse
    * @return ZipInputstream of the zip contents
    *
    *
    * TODO: Because we're only handling zips in this class,
    * and they should already be filtered by the FilenameFilter,
    * I wonder if we even need the match statement here.
    */
  def getInputStream(file: File): Option[ZipInputStream] = {
    file.getName match {
      case zipName if zipName.endsWith("zip") =>
        Some(new ZipInputStream(new FileInputStream(file)))
      case _ => None
    }
  }

  /**
    * Main logic for handling individual entries in the zip.
    *
    * @param zipResult  Case class representing extracted item from the zip
    * @return Count of metadata items found.
    */
  def handleFile(zipResult: FileResult,
                 unixEpoch: Long): Try[Int] =
    zipResult.data match {
      case None =>
        Success(0) //a directory, no results

      case Some(data) => Try {
        Try {
          XML.loadString(new String(data)) // parse string to XML
        } match {
          case Success(xml) =>
            val id: String = zipResult.entryName
            val outputXml: String = xmlToString(xml)
            val item: ParsedResult = ParsedResult(id, outputXml)
            writeOut(unixEpoch, item)
            1
          case _ => 0
        }
      }
    }

  /**
    * Implements a stream of files from the zip
    * Can't use @tailrec here because the compiler can't recognize it as tail recursive,
    * but this won't blow the stack.
    *
    * @param zipInputStream
    * @return Lazy stream of zip records
    */
  def iter(zipInputStream: ZipInputStream): Stream[FileResult] =
    Option(zipInputStream.getNextEntry) match {
      case None =>
        Stream.empty
      case Some(entry) =>
        val result =
          if (entry.isDirectory || !entry.getName.endsWith(".xml"))
            None
          else {
            Some(IOUtils.toByteArray(zipInputStream, entry.getSize))
          }
        FileResult(entry.getName, result) #:: iter(zipInputStream)
    }

  /**
    * Executes the Digital Virginias harvest
    */
  override def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles.listFiles(new ZipFileFilter).foreach( inFile => {
      val inputStream = getInputStream(inFile)
        .getOrElse(throw new IllegalArgumentException("Couldn't load ZIP files."))
      val recordCount = (for (result <- iter(inputStream)) yield {
        handleFile(result, unixEpoch) match {
          case Failure(exception) =>
            logger.error(s"Caught exception on $inFile.", exception)
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
    spark.read.avro(tmpOutStr)
  }

  /**
    * Converts a Node to an xml string
    *
    * @param node The root of the tree to write to a string
    * @return a String containing xml
    */
  def xmlToString(node: Node): String =
    Utility.serialize(node, minimizeTags = MinimizeMode.Always).toString
}
