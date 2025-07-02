package dpla.ingestion3.harvesters.file

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.file.FileFilters.gzFilter
import dpla.ingestion3.harvesters.{FileResult, Harvester, LocalHarvester, ParsedResult}
import dpla.ingestion3.mappers.utils.XmlExtractor
import dpla.ingestion3.model.AVRO_MIME_XML
import org.apache.avro.generic.GenericData
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File
import java.nio.charset.Charset
import scala.util.{Failure, Success, Try, Using}
import scala.xml._

class HathiFileHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(shortName, conf)
    with XmlExtractor {

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

  /** Executes the harvest
    */
  override def harvest: DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles
      .listFiles(gzFilter)
      .foreach(inFile =>
        Using(
          LocalHarvester
            .getTarInputStream(inFile)
            .getOrElse(
              throw new IllegalArgumentException(
                s"Couldn't load file, ${inFile.getAbsolutePath}"
              )
            )
        ) { inputStream =>
          LocalHarvester
            .iter(inputStream)
            .foreach(tarResult => {
              handleFile(tarResult, unixEpoch) match {
                case Failure(exception) =>
                  logger
                    .error(
                      s"Caught exception on ${tarResult.entryName}.",
                      exception
                    )
                case _ => // do nothing
              }
            })
        }
      )

    // flush the avroWriter
    close()

    // Read harvested data into Spark DataFrame and return.
    spark.read.format("avro").load(tmpOutStr)
  }

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
          val dataString = new String(data, Charset.forName("UTF-8")).replaceAll("<\\?xml.*\\?>", "").trim

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
