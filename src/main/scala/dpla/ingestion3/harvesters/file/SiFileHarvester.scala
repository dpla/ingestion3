package dpla.ingestion3.harvesters.file

import java.io.{File, FileFilter, FileInputStream, InputStreamReader}
import java.util.zip.GZIPInputStream

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.utils.Utils
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source
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
    * Get the expected record counts for each provided file from a text file provided along side metadata. Contents of
    * file are expected to match this format:
    *
    *   AAADCD_DPLA.xml records = 15,234
    *   ACAH_DPLA.xml records = 0
    *   ACM_DPLA.xml records = 1,500
    *   CHNDM_DPLA.xml records = 156,226
    *
    * @param inFiles File           Input directory with metadata and summary file
    * @return Map[String, String]   Map of filename to record count (formatted as string in file)
    */
  def getExpectedFileCounts(inFiles: File): Map[String, String] = {
    var loadCounts = Map[String, String]()
    inFiles.listFiles(new TxtFileFilter).foreach(file => {
      Source.fromFile(file).getLines().foreach(line => {
        val lineVals = line.split(" records = ")
        loadCounts += (lineVals(0).replace(".xml", ".gz") -> lineVals(1)) // rename .xml to .gz to match filename processed by harvester
      })
    })
    loadCounts
  }

  /**
    * Executes the Smithsonian harvest
    */
  override def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    // Smithsonian now provides expected harvest counts in a .txt file alongside the compressed metadata, get those
    // file names and counts for comparison to harvested data. Should help narrow down any issues with harvest mismatch
    // counts
    val expectedFileCounts = getExpectedFileCounts(inFiles)

    inFiles.listFiles(new GzFileFilter).foreach( inFile => {
      val inputStream = getInputStream(inFile)
        .getOrElse(throw new IllegalArgumentException(s"Couldn't load file, ${inFile.getAbsolutePath}"))

      // create lineIterator to read contents one line at a time
      val iter = IOUtils.lineIterator(inputStream)

      var lineCount: Int = 0

      while (iter.hasNext) {
       Option(iter.nextLine) match {
          case Some(line) => lineCount += handleLine(line, unixEpoch).get
          case None => 0
        }
      }
      IOUtils.closeQuietly(inputStream)

      // Format the harvested and expected counts for logging
      val fromFileFloat  = lineCount.toFloat
      val fromFilePretty = Utils.formatNumber(fromFileFloat.toLong)
      val expectedPretty = expectedFileCounts.getOrElse(inFile.getName, "0")
      val expectedFloat  = expectedPretty.replaceAll(",","").trim.toFloat // remove commas from string and make float
      val percentage     = (fromFileFloat / expectedFloat) * 100.0f match {
        case x if x.isNaN => 0.0f // account for division by 0...
        case x if !x.isNaN => x
      }

      // Log a summary of the file
      logger.info(s"Harvested $percentage% ($fromFilePretty / $expectedPretty) of records from ${inFile.getName}")
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

class TxtFileFilter extends FileFilter {
  override def accept(pathname: File): Boolean = pathname.getName.endsWith("txt")
}