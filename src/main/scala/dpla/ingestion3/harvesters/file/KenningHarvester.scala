package dpla.ingestion3.harvesters.file

import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.LocalHarvester
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.jackson.JsonMethods._
import org.json4s.{JValue, _}

import java.io.File
import scala.util.{Failure, Success, Try}


class KenningHarvester(spark: SparkSession,
                       shortName: String,
                       conf: i3Conf,
                       logger: Logger)
  extends FileHarvester(spark, shortName, conf, logger) {

  // TODO this is defined as a template method in FileHarvester
  // but you have to call it manually from the implementing class
  // Should probably be eliminated as an abstract method.
  override def handleFile(fileResult: FileResult, unixEpoch: Long): Try[Int] = Failure(new NotImplementedError("GFY"))

  def mimeType: String = "application_json"

  protected val extractor = new CsvFileExtractor()

  override def localHarvest(): DataFrame = {
    val harvestTime = System.currentTimeMillis()
    val unixEpoch = harvestTime / 1000L
    val inFiles = new File(conf.harvest.endpoint.getOrElse("in"))
      .listFiles(new CsvFileFilter)
      .map(_.getAbsolutePath)
      .toSeq

    val data = spark
      .read
      .format("csv")
      .load(inFiles: _*).collect()

    var count = 0

    for {
      row <- data
      array = JArray(row.toSeq.map(value => JString(value.toString)).toList)
      record = compact(render(array))
    } {
      writeOut(unixEpoch, ParsedResult(count.toString, record))
      count += 1
    }

    flush()

    spark.read.avro(tmpOutStr)
  }
}

//case class CsvRow (id: Option[String], data: Option[String])
