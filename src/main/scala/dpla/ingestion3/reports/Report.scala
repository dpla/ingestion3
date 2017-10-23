package dpla.ingestion3.reports


import java.io.File

import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.sql._

import scala.util.Try

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

  private final val __MISSING__ = "__MISSING__"
  val sparkAppName: String = "Report"  // Usually overridden

  /*
   * Accessor methods are used to make the trait and its extending classes
   * more amenable to unit testing.
   */
  def getInputURI: String
  def getOutputURI: String
  def getParams: Option[Array[String]]
  def getSparkConf: SparkConf

  /**
    * Run the report, opening the input and output and invoking the process()
    * function in the extending class.
    *
    * @see Reporter.main()
    * @return Try object representing success or failure, for Reporter.main()
    */
  def run(): Try[Unit] = Try {
    val spark = SparkSession
      .builder()
      .config(getSparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._

    implicit val dplaMapDataEncoder: Encoder[OreAggregation] =
      org.apache.spark.sql.Encoders.kryo[OreAggregation]

    val input = spark
        .read
        .format("com.databricks.spark.avro")
        .load(getInputURI).map(row => ModelConverter.toModel(row))

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
  def process(ds: Dataset[OreAggregation], spark: SparkSession): DataFrame

  /**
    * Accepts Seq[Any] and tries to extract the String values which
    * should be included in the report.
    *
    * For fields like Title and Rights this is fairly straight forward but for
    * context case classes like EdmAgent and SkosConcept it will be slightly trickier
    * to report on multiple fields.
    *
    *  If the sequence is empty then a String indicating that the value is missing is
    * returned.
    *
    * @param t Sequence to report on
    * @return
    */
  def extractValue(t: Seq[AnyRef]): Seq[String] = {
    if (t.isEmpty) {
      Seq(__MISSING__)
    } else {
      t.head match {
        case _: DplaPlace =>
          t.map(_.asInstanceOf[DplaPlace].name.getOrElse("__MISSING DplaPlace.name__"))
        case _: DcmiTypeCollection =>
          t.map(_.asInstanceOf[DcmiTypeCollection].title.getOrElse("__MISSING DcmiTypeCollection.title__"))
        case _: EdmAgent =>
          t.map(_.asInstanceOf[EdmAgent].name.getOrElse("__MISSING EdmAgent.name__"))
        case _: EdmTimeSpan =>
          t.map(_.asInstanceOf[EdmTimeSpan].originalSourceDate.getOrElse("__MISSING EdmTimeSpan.originalSourceDate__"))
        case _: SkosConcept =>
          t.map(_.asInstanceOf[SkosConcept].providedLabel.getOrElse("__MISSING SkosConcept.providedLabel__"))
        case _ => t.map(_.toString)
      }
    }
  }

}
