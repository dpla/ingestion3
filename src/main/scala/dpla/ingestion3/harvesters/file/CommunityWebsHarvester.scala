package dpla.ingestion3.harvesters.file

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.file.FileFilters.zipFilter
import dpla.ingestion3.mappers.utils.JsonExtractor
import dpla.ingestion3.model.AVRO_MIME_JSON
import org.apache.avro.generic.GenericData
import org.apache.commons.io.IOUtils
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.JsonMethods._
import org.json4s.{JValue, _}

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.util.zip.ZipInputStream
import scala.util.{Failure, Success, Try}

/** Entry for performing a Community Webs file harvest
  */
class CommunityWebsHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends FileHarvester(spark, shortName, conf) {

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_JSON

  protected val extractor = new FlFileExtractor()

  /** Parses JValue to extract item local item id and renders compact full
    * record
    *
    * @param json
    *   Full JSON item record
    * @return
    *   Option[ParsedResult]
    */
  def getJsonResult(json: JValue): Option[ParsedResult] =
    Option(
      ParsedResult(
        extractor
          .extractString(json \ "id")
          .getOrElse(throw new RuntimeException("Missing ID")),
        compact(render(json))
      )
    )

  /** Parses and extracts ZipInputStream and writes parsed records out.
    *
    * @param zipResult
    *   Case class representing extracted items from the zip
    * @return
    *   Count of metadata items found.
    */
  def handleFile(zipResult: FileResult, unixEpoch: Long): Try[Int] = {

    var itemCount: Int = 0

    zipResult.bufferedData match {
      case None =>
        Success(0) // a directory, no results
      case Some(data) =>
        Try {

          //  FL now provides JSONL (one record per line)
          var line: String = data.readLine

          while (line != null) {
            val count = Try {

              // Clean up leading/trailing characters
              val json: JValue = parse(line.stripPrefix("[").stripPrefix(","))

              getJsonResult(json) match {
                case Some(item) =>
                  writeOut(unixEpoch, item)
                  1
                case _ => 0
              }
            } match {
              case Success(num) => num
              case _            => 0
            }

            itemCount += count
            line = data.readLine
          }
          itemCount
        }
    }
  }


  override def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles
      .listFiles(zipFilter)
      .foreach(inFile => {
        val inputStream: ZipInputStream = FileHarvester.getZipInputStream(inFile)
          .getOrElse(
            throw new IllegalArgumentException("Couldn't load ZIP files.")
          )
        FileHarvester.iter(inputStream).foreach(result =>  {
          handleFile(result, unixEpoch) match {
            case Failure(exception) =>
              LogManager
                .getLogger(this.getClass)
                .error(s"Caught exception on $inFile.", exception)
            case _ => // do nothing
          }
        })
        IOUtils.closeQuietly(inputStream)
      })

    getAvroWriter.flush()

    // Read harvested data into Spark DataFrame.
    val df = spark.read.format("avro").load(tmpOutStr)

    // Filter out records with "status":"deleted"
    df.where(!col("document").like("%\"status\":\"deleted\"%"))
  }
}
