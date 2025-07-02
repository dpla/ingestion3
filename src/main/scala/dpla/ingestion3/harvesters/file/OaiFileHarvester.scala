package dpla.ingestion3.harvesters.file

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{
  FileResult,
  Harvester,
  LocalHarvester,
  ParsedResult
}
import dpla.ingestion3.mappers.utils.XmlExtractor
import dpla.ingestion3.model.AVRO_MIME_XML
import org.apache.avro.generic.GenericData
import org.apache.commons.io.IOUtils
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{ByteArrayInputStream, File}
import scala.util.{Failure, Success, Try}
import scala.xml._

/** Entry for harvesting XML serialized from an OAI endpoint
  */
class OaiFileHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(shortName, conf)
    with XmlExtractor {

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_XML

  private val logger = LogManager.getLogger(this.getClass)

  /** Main logic for handling individual entries in the zip.
    *
    * @param zipResult
    *   Case class representing extracted item from the zip
    * @return
    *   Count of metadata items found.
    */
  def handleFile(zipResult: FileResult, unixEpoch: Long): Try[Int] = {
    val xmlOption =
      if (zipResult.data.isDefined)
        Some(XML.load(new ByteArrayInputStream(zipResult.data.get)))
      else
        None
    xmlOption match {
      case None =>
        Success(0) // a directory, no results
      case Some(xml) =>
        Try {
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

  /** Takes care of parsing an XML file into a list of Nodes each representing
    * an item
    *
    * @param xml
    *   Root of the xml document
    * @return
    *   List of Options of id/item pairs.
    */
  def handleXML(xml: Node): List[Option[ParsedResult]] = {

    for {
      items <- xml \\ "record" :: Nil
      item <- items
    } yield item match {
      case record: Node =>
        if (
          (record \ "header").head
            .attribute("status")
            .getOrElse(<foo></foo>)
            .text
            .equalsIgnoreCase("deleted")
        ) {
          None
        } else {
          // Extract required record identifier
          val id: Option[String] = extractString(record \ "header" \ "identifier")
          val outputXML = Harvester.xmlToString(record)

          id match {
            case None =>
              logger.warn(s"Missing required record_ID for $outputXML")
              None
            case Some(id) => Some(ParsedResult(id, outputXML))
          }
        }
      case _ =>
        logger.warn("Got weird result back for item path: " + item.getClass)
        None
    }
  }

  override def harvest: DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles
      .listFiles(FileFilters.zipFilter)
      .foreach(inFile => {
        val inputStream = LocalHarvester
          .getZipInputStream(inFile)
          .getOrElse(
            throw new IllegalArgumentException("Couldn't load ZIP files.")
          )
        LocalHarvester
          .iter(inputStream)
          .foreach(result =>
            handleFile(result, unixEpoch) match {
              case Failure(exception) =>
                logger.error(s"Caught exception on $inFile.", exception)
              case Success(_) => // do nothing
            }
          )
        IOUtils.closeQuietly(inputStream)
      })

    close()

    // Read harvested data into Spark DataFrame and return.
    spark.read.format("avro").load(tmpOutStr)
  }

}
