package dpla.ingestion3.harvesters.file

import java.io.{File, FileInputStream, InputStreamReader}
import java.util.zip.GZIPInputStream

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Success, Try}
import scala.xml.{MinimizeMode, Node, Utility, XML}

/**
  * Entry for performing Smithsonian file harvest
  */
class SiFileHarvester(spark: SparkSession,
                      shortName: String,
                      conf: i3Conf,
                      logger: Logger)
  extends FileHarvester(spark, shortName, conf, logger) {

  def mimeType: String = "application_xml"

  /**
    * Loads .gz files
    *
    * @param file File to parse
    * @return Option[InputStreamReader] of the zip contents
    *
    *
    * TODO: Because we're only handling zips in this class,
    * and they should already be filtered by the FilenameFilter,
    * I wonder if we even need the match statement here.
    */
  def getInputStream(file: File): Option[InputStreamReader] = {
    file.getName match {
      case zipName if zipName.endsWith("gz") =>
        Some(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))
      case _ => None
    }
  }

  /**
    * Takes care of parsing an xml file into a list of Nodes each representing an item
    *
    * @param xml Root of the xml document
    * @return List of Options of id/item pairs.
    */
  def handleXML(xml: Node): List[Option[ParsedResult]] = {
    for {
      items <- xml \\ "doc" :: Nil
      item <- items
    } yield item match {
      case record: Node =>
        // Extract required record identifier
        val id = Option((record \ "descriptiveNonRepeating" \ "record_ID").text.toString)
        val outputXML = xmlToString(record)

        id match {
          case (None) =>
            logger.warn(s"Missing required record_ID for $outputXML")
            None
          case (Some(id)) => Some(ParsedResult(id, outputXML))
        }
      case _ =>
        logger.warn("Got weird result back for item path: " + item.getClass)
        None
    }
  }

  /**
    * Main logic for handling individual lines in the zipped file.
    *
    * @param line String line from file
    * @return Count of metadata items found.
    */
  def handleLine(line: String,
                 unixEpoch: Long): Try[Int] =

    Option(line) match {
      case None =>
        Success(0) //a directory, no results

      case Some(data) =>
        Try {
          val dataString = new String(data).replaceAll("<\\?xml.*\\?>", "")
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

  /**
    * Executes the Smithsonian harvest
    */
  override def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles.listFiles(new GzFileFilter).foreach( inFile => {
      logger.info(s"Reading ${inFile.getName}")
      val inputStream = getInputStream(inFile)
        .getOrElse(throw new IllegalArgumentException(s"Couldn't load file, ${inFile.getAbsolutePath}"))

      // create lineIterator to read contents one line at a time
      val iter = IOUtils.lineIterator(inputStream)
      while (iter.hasNext) {
       Option(iter.nextLine) match {
         case Some(line) => handleLine(line, unixEpoch)
         case None => 0
       }
      }
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

  /**
    * Parses and extracts ZipInputStream and writes
    * parses records out.
    *
    * @param fileResult Case class representing extracted items from a compressed file
    * @return Count of metadata items found.
    */
  override def handleFile(fileResult: FileResult, unixEpoch: Long): Try[Int] = ???
}
