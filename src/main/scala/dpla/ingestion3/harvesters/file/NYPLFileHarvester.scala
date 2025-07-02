package dpla.ingestion3.harvesters.file

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.util.zip.ZipInputStream
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{FileResult, LocalHarvester, ParsedResult}
import dpla.ingestion3.harvesters.file.FileFilters.zipFilter
import dpla.ingestion3.harvesters.oai.LocalOaiHarvester
import dpla.ingestion3.mappers.utils.JsonExtractor
import dpla.ingestion3.model.AVRO_MIME_JSON
import org.apache.avro.generic.GenericData
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.JsonMethods._
import org.json4s.{JValue, _}

import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/** Entry for performing an NYPL file harvest
  */
class NYPLFileHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(shortName, conf)
    with JsonExtractor {

  // The format of the exported data is JSON but the underlying records we will need to map is XML
  // {
  //  "uuid": "f30c5ae0-f14a-0134-cd0c-6111f36df79b",
  //  "desc_xml": "<?xml version=\"1.0\" .... </xml>"
  // }
  def mimeType: GenericData.EnumSymbol = AVRO_MIME_JSON

  /** @param json
    *   Full JSON item record
    * @return
    *   Option[ParsedResult]
    */
  def getJsonResult(json: JValue): Option[ParsedResult] = {
    // The record id is extract from the JSON but the complete record is stored as XML
    // {
    //  "uuid": "f30c5ae0-f14a-0134-cd0c-6111f36df79b",
    //  "desc_xml": "<?xml version=\"1.0\" .... </xml>"
    // }

    extractString(json \ "uuid")
      .getOrElse(throw new RuntimeException("Missing ID"))

    Option(
      ParsedResult(
        extractString(json \ "uuid")
          .getOrElse(throw new RuntimeException("Missing ID")),
        compact(render(json))
      )
    )
  }

  /** Parses and extracts ZipInputStream and writes parsed records out.
    *
    * @param zipResult
    *   Case class representing extracted items from the zip
    * @return
    *   Count of metadata items found.
    */
  def handleFile(zipResult: FileResult, unixEpoch: Long): Try[Int] = {

    var itemCount: Int = 0

    zipResult.data match {
      case None =>
        Success(0) // a directory, no results
      case Some(data) =>
        Using(Source.fromBytes(data)) { source =>
          for (line <- source.getLines) {
            val count = Try {
              // Clean up leading/trailing characters
              val json: JValue = parse(line.stripPrefix("[").stripPrefix(","))
              getJsonResult(json) match {
                case Some(item) =>
                  writeOut(unixEpoch, item)
                  1
                case _ => 0
              }
            }.getOrElse(0)
            itemCount += count
          }
          itemCount
        }
    }
  }

  /** Executes the NYPL harvest
    */
  override def harvest: DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles
      .listFiles(zipFilter)
      .foreach(inFile => {
        val inputStream: ZipInputStream = LocalHarvester
          .getZipInputStream(inFile)
          .getOrElse(
            throw new IllegalArgumentException("Couldn't load ZIP files.")
          )
        LocalHarvester
          .iter(inputStream)
          .foreach(result =>
            handleFile(result, unixEpoch) match {
              case Failure(exception) =>
                LogManager
                  .getLogger(this.getClass)
                  .error(s"Caught exception on $inFile.", exception)
              case Success(_) => // do nothing
            }
          )
        IOUtils.closeQuietly(inputStream)
      })

    close()

    // Read harvested data into Spark DataFrame.
    spark.read.format("avro").load(tmpOutStr)
  }
}
