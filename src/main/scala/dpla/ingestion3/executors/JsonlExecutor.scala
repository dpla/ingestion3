package dpla.ingestion3.executors

import java.io.File

import dpla.ingestion3.model.{ModelConverter, jsonlRecord}
import dpla.ingestion3.utils.Utils
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

trait JsonlExecutor extends Serializable {

  /**
    * Generate JSON-L files from AVRO file
    * @param sparkConf Spark configuration
    * @param dataIn Data to transform into JSON-L
    * @param dataOut Location to save JSON-L
    */
  def executeJsonl(sparkConf: SparkConf,
              dataIn: String,
              dataOut: String,
              logger: Logger): Unit = {

    logger.info("Starting JSON-L export")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val enrichedRows =
      spark.read
        .format("com.databricks.spark.avro")
        .load(dataIn)

    val indexRecords: Dataset[String] = enrichedRows.map(
      row => {
        val record = ModelConverter.toModel(row)
        jsonlRecord(record)
      }
    )

    // This should always write out as #text() because if we use #json() then the
    // data will be written out inside a JSON object (e.g. {'value': <doc>}) which is
    // invalid for our use
    indexRecords.coalesce(1).write.text(dataOut)
    sc.stop()

    logger.info("JSON-L export complete")
  }
}
