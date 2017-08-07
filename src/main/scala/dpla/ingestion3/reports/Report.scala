package dpla.ingestion3.reports


import java.io.File
import com.databricks.spark.avro._
import dpla.ingestion3.model.DplaMapData
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import dpla.ingestion3.utils.Utils
import util.Try


/**
  * Report:  trait with common fields and methods for QA reports.
  *
  * Report handles the common work of loading the input Avro file and writing
  * the result of the report's computation out to a CSV file.
  *
  * It stipulates an abstract method process() that must be overridden by
  * extending classes to do the actual computation.
  *
  * The implementation of Report as a Trait was supposed to make it easier to
  * unit test, but, it's been difficult to envision how to mock up the Spark
  * objects and stub their method calls, and create fixtures for the Dataset
  * and Dataframe.  This could be grounds for future work.
  *
  * TODO: per comment above, make fixtures and tests for run()?
  */
trait Report {

  val sparkAppName: String = "Report"  // Usually overridden

  /*
   * Accessor methods are used to make the trait and its extending classes
   * more amenable to unit testing.
   */
  def getInputURI: String
  def getOutputURI: String
  def getSparkMasterName: String
  def getParams: Option[Array[String]]

  /**
    * Run the report, opening the input and output and invoking the process()
    * function in the extending class.
    *
    * @see Reporter.main()
    * @return Try object representing success or failure, for Reporter.main()
    */
  def run(): Try[Unit] = Try {

    val sparkConf: SparkConf =
      new SparkConf().setAppName(sparkAppName).setMaster(getSparkMasterName)
    sparkConf.set(
      "spark.serializer", "org.apache.spark.serializer.KryoSerializer"
    )
    implicit val dplaMapDataEncoder =
      org.apache.spark.sql.Encoders.kryo[DplaMapData]
    val spark: SparkSession =
      SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val input: Dataset[DplaMapData] =
      spark.read.avro(getInputURI).as[DplaMapData]
    val output: DataFrame = process(input, spark)

    Utils.deleteRecursively(new File(getOutputURI))
    output
      .repartition(1)  // Otherwise multiple CSV files
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(getOutputURI)

    sc.stop()

  }

  /**
    * Process the incoming dataset (mapped or enriched records) and return a
    * DataFrame of computed results, typically values extracted from the
    * records and their counts.
    *
    * Overridden by classes in dpla.ingestion3.reports
    *
    * @param ds     Dataset of DplaMapData (mapped or enriched records)
    * @param spark  The Spark session, which contains encoding / parsing info.
    * @return       DataFrame, typically of Row[value: String, count: Int]
    */
  def process(ds: Dataset[DplaMapData], spark: SparkSession): DataFrame

}
