package dpla.ingestion3.harvesters.file

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.util.zip.ZipInputStream
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.file.FileFilters.ZipFileFilter
import dpla.ingestion3.mappers.utils.JsonExtractor
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.native.JsonMethods._
import org.json4s.{JValue, _}

import scala.util.{Failure, Success, Try}

/**
  * Extracts values from parsed JSON
  */
class DlgFileExtractor extends JsonExtractor

/**
  * Entry for performing a Georgia file harvest
  */
class DlgFileHarvester(spark: SparkSession,
                       shortName: String,
                       conf: i3Conf,
                       logger: Logger)
  extends FileHarvester(spark, shortName, conf, logger) {

  def mimeType: String = "application_json"

  protected val extractor = new DlgFileExtractor()

  /**
    * Loads .zip files
    *
    * @param file File to parse
    * @return ZipInputstream of the zip contents
    */
  def getInputStream(file: File): Option[ZipInputStream] = {
    file.getName match {
      case zipName if zipName.endsWith("zip") =>
        Some(new ZipInputStream(new FileInputStream(file)))
      case _ => None
    }
  }

  /**
    * Parses JValue to extract item local item id and renders compact
    * full record
    *
    * @param json Full JSON item record
    * @return Option[ParsedResult]
    */
  def getJsonResult(json: JValue): Option[ParsedResult] =
    Option(ParsedResult(
      extractor.extractString(json \ "id")
        .getOrElse(throw new RuntimeException("Missing ID")),
      compact(render(json))
    ))

  /**
    * Parses and extracts ZipInputStream and writes
    * parsed records out.
    *
    * @param zipResult  Case class representing extracted items from the zip
    * @return Count of metadata items found.
    */
  def handleFile(zipResult: FileResult,
                 unixEpoch: Long): Try[Int] = {

    zipResult.bufferedData match {
      case None =>
        Success(0) // a directory, no results
      case Some(data) => Try {

        // Assume that each line of the file contains a single record.
        var line: String = data.readLine
        var itemCount: Int = 0

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
            case _ => 0
          }

          itemCount += count
          line = data.readLine
        }

        itemCount
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
          if (entry.isDirectory)
            None
          else
            Some(new BufferedReader(new InputStreamReader(zipInputStream)))
        FileResult(entry.getName, None, result) #:: iter(zipInputStream)
    }

  /**
    * Executes the Georgia harvest
    */
  override def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles.listFiles(new ZipFileFilter).foreach( inFile => {
      val inputStream: ZipInputStream = getInputStream(inFile)
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

    // Read harvested data into Spark DataFrame.
    val df = spark.read.format("avro").load(tmpOutStr)

    flush()

    // Filter out records with "status":"deleted"
    df.where(!col("document").like("%\"status\":\"deleted\"%"))
  }
}
