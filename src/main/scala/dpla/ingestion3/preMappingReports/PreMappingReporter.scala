package dpla.ingestion3.premappingreports

import java.io.File

import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.spark.avro._

import scala.util.Try

/**
  * This class reads in harvested data.
  * It calls PreMappingReport to generate reports.
  * It writes out the reports.
  *
  * @param inputDir Path to harvested data.
  * @param outputDir Path to pre-mapping reports.
  * @param sparkMasterName
  * @param inputDataType "json", "xml"
  */
class PreMappingReporter(val inputDir: String,
                             val outputDir: String,
                             val sparkMasterName: String,
                             val inputDataType: String) extends Serializable {

  private val spark: SparkSession = {
    val sparkConf: SparkConf =
      new SparkConf().setAppName("PreMappingReport").setMaster(sparkMasterName)
    SparkSession.builder().config(sparkConf).getOrCreate()
  }

  private val report = {
    val input: DataFrame = spark.read.avro(inputDir)
    new PreMappingReport(input, inputDataType, spark)
  }

  private val outputDirName: String = {
    if (outputDir.endsWith("/")) outputDir.dropRight(1)
    else outputDir
  }

  // Main entry point.
  def writeAllReports: Try[Unit] = Try {

    // If the output directory already exists it is a local path
    // then delete it and its contents.
    val outputFile = new File(outputDir)
    outputFile.getParentFile.mkdirs()
    if (outputFile.exists & !outputDir.startsWith("s3"))
      Utils.deleteRecursively(outputFile)

    // Write processed data.
    // TODO: Determine whether we want to write out all of reports or allow
    // user to choose which reports they want to generate.  We may also decide
    // to eliminate some of these reports if we don't need them.
    writeShreddedData
    writeElasticSearchData
    writeCardinalityReport
    writeOpenRefineData
    writeRecordsMissingAttributesReport

    // Stop spark context.
    spark.sparkContext.stop()
  }

  private def writeShreddedData(): Unit =
    report.tripleShreds
      .write
      .parquet(s"$outputDirName/shreddedData")

  private def writeElasticSearchData(): Unit =
    report.elasticSearchData
      .coalesce(1)
      .saveAsTextFile(s"$outputDirName/elasticSearchFormattedData")

  private def writeCardinalityReport(): Unit =
    report.cardinality
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(s"$outputDirName/cardinalityReport")

  private def writeOpenRefineData(): Unit =
    report.openRefineData
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(s"$outputDirName/openRefineData")

  private def writeRecordsMissingAttributesReport(): Unit =
    report.recordsMissingAttributes
      .coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(s"$outputDirName/recordsMissingAttributes")
}
