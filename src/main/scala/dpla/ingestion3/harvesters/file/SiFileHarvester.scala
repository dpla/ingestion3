package dpla.ingestion3.harvesters.file

import java.io.{
  ByteArrayInputStream,
  File,
  FileFilter,
  FileInputStream,
  InputStreamReader
}
import java.util.zip.GZIPInputStream
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{Harvester, LocalHarvester, ParsedResult}
import dpla.ingestion3.harvesters.file.FileFilters.gzFilter
import dpla.ingestion3.model.AVRO_MIME_XML
import dpla.ingestion3.utils.Utils
import org.apache.avro.generic.GenericData
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source
import scala.util.{Failure, Success, Try, Using}
import scala.xml.{MinimizeMode, Node, Utility, XML}

/** Entry for performing Smithsonian file harvest
  */
class SiFileHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(shortName, conf) {

  private val logger = LogManager.getLogger(this.getClass)

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML

  /** Takes care of parsing an xml file into a list of Nodes each representing
    * an item
    *
    * @param xml
    *   Root of the xml document
    * @return
    *   List of Options of id/item pairs.
    */
  def handleXML(xml: Node): List[Option[ParsedResult]] =
    for {
      items <- xml \\ "doc" :: Nil
      item <- items
    } yield item match {
      case record: Node =>
        // Extract required record identifier
        val id = Option(
          (record \ "descriptiveNonRepeating" \ "record_ID").text
        )
        val outputXML = Harvester.xmlToString(record)

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

  /** Main logic for handling individual lines in the zipped file.
    *
    * @param line
    *   String line from file
    * @return
    *   Count of metadata items found.
    */
  private def handleLine(line: String, unixEpoch: Long): Try[Int] =
    Option(line) match {
      case None =>
        Success(0) // a directory, no results

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

  /** Get the expected record counts for each provided file from a text file
    * provided along side metadata. Contents of file are expected to match this
    * format:
    *
    * AAADCD_DPLA.xml records = 15,234 ACAH_DPLA.xml records = 0 ACM_DPLA.xml
    * records = 1,500 CHNDM_DPLA.xml records = 156,226
    *
    * @param inFiles
    *   File Input directory with metadata and summary file
    * @return
    *   Map[String, String] Map of filename to record count (formatted as string
    *   in file)
    */
  private def getExpectedFileCounts(inFiles: File): Map[String, String] = {
    var loadCounts = Map[String, String]()
    inFiles
      .listFiles(FileFilters.txtFilter)
      .foreach(file => {
        Using(
          Source
            .fromFile(file)
        ) { source =>
          source
            .getLines()
            .foreach(line => {
              Try {
                val lineVals = line.split(" records = ")
                // rename .xml to .xml.gz to match filename processed by harvester
                lineVals(0).replace(".xml", ".xml.gz") -> lineVals(1)
              } match {
                case Success(row) => loadCounts += row
                case Failure(_)   => loadCounts
              }
            })
        }
      })
    loadCounts
  }

  /** Executes the Smithsonian harvest
    */
  override def harvest: DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    // Smithsonian now provides expected harvest counts in a .txt file alongside the compressed metadata, get those
    // file names and counts for comparison to harvested data. Should help narrow down any issues with harvest mismatch
    // counts
    val expectedFileCounts = getExpectedFileCounts(inFiles)

    inFiles
      .listFiles(gzFilter)
      .foreach(inFile => {
        println("FILE: " + inFile.getAbsolutePath)
        var lineCount: Int = 0
        Using(
          new GZIPInputStream(new FileInputStream(inFile))
        ) { inputStream =>
          val iter = IOUtils.lineIterator(inputStream, "utf-8")
          while (iter.hasNext) {
            Option(iter.nextLine) match {
              case Some(line) =>
                // Remove XML declaration and whitespace, then parse the line
                lineCount += handleLine(line, unixEpoch).get
              case None => 0
            }
          }
        }

        // Format the harvested and expected counts for logging
        val fromFileFloat = lineCount.toFloat
        val fromFilePretty = Utils.formatNumber(fromFileFloat.toLong)
        val getFilenameKey = (filename: String) =>
          filename.substring(0, inFile.getName.indexOf("."))
        val expectedPretty =
          expectedFileCounts.getOrElse(getFilenameKey(inFile.getName), "0")
        val expectedFloat =
          expectedPretty
            .replaceAll(",", "")
            .trim
            .toFloat // remove commas from string and make float
        val percentage = (fromFileFloat / expectedFloat) * 100.0f match {
          case x if x.isNaN      => 0.0f // account for NaN
          case x if x.isInfinity => fromFileFloat // account for infinity
          case x if !x.isNaN     => x.intValue()
        }

        // Log a summary of the file
        LogManager
          .getLogger(this.getClass)
          .info(
            s"Harvested $percentage% ($fromFilePretty / $expectedPretty) of records from ${inFile.getName}"
          )
      })

    close()

    // Read harvested data into Spark DataFrame and return.
    spark.read.format("avro").load(tmpOutStr)
  }

}
