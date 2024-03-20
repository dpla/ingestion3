package dpla.ingestion3.harvesters.file

import java.io.File

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.file.FileFilters.CsvFileFilter
import dpla.ingestion3.mappers.utils.JsonExtractor
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.JsonMethods._
import org.json4s.{JValue, _}

import scala.io.Source
import scala.util.{Failure, Success, Try}


/**
  * Extracts values from parsed JSON
  */
class CsvFileExtractor extends JsonExtractor

/**
  * Entry for performing the LC CSV file harvest
  */
class LcCsvFileHarvester(spark: SparkSession,
                         shortName: String,
                         conf: i3Conf,
                         logger: Logger)
  extends FileHarvester(spark, shortName, conf, logger) {

  def mimeType: String = "application_json"

  protected val extractor = new CsvFileExtractor()

  /**
    * Loads .csv files
    *
    * @param file File to parse
    * @return Iterator[String] of the CSV lines
    */
  def getInputStream(file: File): Option[Iterator[String]] = {
    file.getName match {
      case csvName if csvName.endsWith("csv") => // FIXME isn't the redundant of the file filter?
        Some(Source.fromFile(file).getLines())
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
      extractor.extractString(json \\ "id") // Location of the persistent ID for LC records
        .getOrElse(throw new RuntimeException("Missing ID")),
      compact(render(json))
    ))

  /**
    * Parses a row from a CSV file, extracts
    * the data and writes it out
    *
    * @param row  Case class representing extracted items from a CSV row
    * @return Count of metadata items found.
    */
  def handleRow(row: CsvRow,
                 unixEpoch: Long): Try[Int] = {
    row match {
      case CsvRow(_, None) =>
        Success(0) // no data associated with row
      case CsvRow(Some(_), Some(data)) => Try {
        Try {
          // Clean up leading/trailing characters
          val json: JValue = parse(
            data
            .stripPrefix("\"") // remove leading double quote
            .stripSuffix("\"") // remove trailing double quote
            .stripPrefix("[")
            .stripPrefix(",")
            .replaceAll("\"\"", "\"") // replace double-double quote with single double quote
          )

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
      }
    }
  }

  // Leave unimplemented
  override def handleFile(fileResult: FileResult, unixEpoch: Long): Try[Int] = ???

  /**
    *
    * @param iterator Iterator[String] read from file
    * @return Stream[CsvRow] Stream of CsvRows
    */
  def iter(iterator: Iterator[String]): Stream[CsvRow] = {
      if (iterator.hasNext) {
        iterator.next().split(",", 2) match {
        case Array(s1: String, s2: String) =>
          CsvRow(Option(s1), Option(s2)) #:: iter(iterator)
        case _ =>
          Stream.empty
      }
    } else {
        Stream.empty
      }
  }

  /**
    * Executes the Library of Congress file harvest
    */
  override def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))

    inFiles.listFiles(new CsvFileFilter).foreach( inFile => {
      logger.info(s"Reading data from ${inFile.getAbsolutePath}")

      val inputStream: Iterator[String] = getInputStream(inFile)
        .getOrElse(throw new IllegalArgumentException(s"Couldn't load CSV file - ${inFile.getAbsolutePath}"))
      val recordCount = (for (row <- iter(inputStream)) yield {
        handleRow(row, unixEpoch) match {
          case Failure(exception) =>
            logger.error(s"Caught exception on $inFile.", exception)
            0
          case Success(count) =>
            count
        }
      }).sum
    })

    // flush the avroWriter
    flush()

    // Read harvested data into Spark DataFrame.
    spark.read.format("avro").load(tmpOutStr)
  }
}

case class CsvRow (id: Option[String], data: Option[String])
