package dpla.ingestion3.premappingreports

import java.io.File

import com.databricks.spark.avro._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import dpla.ingestion3.utils.Utils
import org.apache.spark.rdd.RDD

import scala.util.parsing.json.JSONType
import util.Try

/**
  * PreMappingReport: trait with common fields and methods for pre-mapping reports.
  *
  * The PreMappingReport trait follow the design patterns of the Report trait.
  */
trait PreMappingReport {

  val sparkAppName: String = "PreMappingReport"  // Usually overridden

  /**
    * Accessor methods are used to make the trait and its extending classes
    * more amenable to unit testing.
    */
  def getInputURI: String
  def getOutputURI: String
  def getSparkMasterName: String

  /**
    * Run the shredder, opening the input and output and invoking the process()
    * function in the extending class.
    *
    * @see PreMappingReporter.main()
    * @return Try object representing success or failure, for PreMappingReporter.main()
    */
  def run(): Try[Unit] = Try {

    val sparkConf: SparkConf =
      new SparkConf().setAppName(sparkAppName).setMaster(getSparkMasterName)
    val spark: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val input: DataFrame = spark.read.avro(getInputURI)

    val output: RDD[JSONType] = process(input)

    Utils.deleteRecursively(new File(getOutputURI))

    output.saveAsTextFile(getOutputURI)

    sc.stop()
  }

  /**
    * Process the incoming DataFrame (harvested records) and return an RDD
    * of computed results.
    *
    * Overridden by classes in dpla.ingestion3.premappingreports
    *
    * @param df     DataFrame of harvested records
    * @return       RDD[JSONType]
    */
  def process(df: DataFrame): RDD[JSONType]
}
