package dpla.ingestion3.reports

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import dpla.ingestion3.model._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, explode}


case class PropertyDistinctValueRpt(value: Seq[String])
/**
  * Property Distinct Value QA report.  Takes one field and gives a count of unique
  * values.
  *
  * Fields currently supported:
  *   - sourceResource.format
  *   - sourceResource.rights
  *   - sourceResource.type
  *
  * @example Example invocation:
  *
  *   $ sbt "run-main dpla.ingestion3.ReporterMain \
  *     /path/to/enriched-data.avro /path/to/propvalreport local[2] \
  *     propertyValue sourceResource.type"
  *
  *     ... This produces a directory named 'propvalreport' with a file in it
  *     named 'part-< ... >.csv', with contents like:
  *
  *     value,count
  *     physicalobject,1
  *     dataset,13
  *     [...]
  *
  * @param inputURI         Input URI or file path (Avro file / directory)
  * @param outputURI        Output URI or file path (directory containing CSV
  *                         file)
  * @param params           Additional parameters, currently:
  *                         params(0): The DPLA MAP field to analyze
  */
class PropertyDistinctValueReport(
                            val inputURI: String,
                            val outputURI: String,
                            val sparkConf: SparkConf,
                            val params: Array[String]
                          ) extends Report with Serializable {

  /*
   * We set instance fields from constructor arguments, and override accessor
   * methods in order to make the class more amenable to unit testing.
   * However ...
   * FIXME: It's not clear how to write unit tests that involve Dataframes
   * and Datasets, so there are no such tests for this class.
   *
   */
  override val sparkAppName: String = "PropertyDistinctValueReport"
  override def getInputURI: String = inputURI
  override def getOutputURI: String = outputURI
  override def getSparkConf: SparkConf = sparkConf
  override def getParams: Option[Array[String]] = {
    if (params.nonEmpty) Some(params) else None
  }

  /**
    * Process the incoming data set.
    *
    * @see          Report.process()
    *
    * @param ds     Dataset of DplaMapData (mapped or enriched records)
    * @param spark  The Spark session, which contains encoding / parsing info.
    * @return       DataFrame, typically of Row[value: String, count: Int]
    */
  override def process(ds: Dataset[OreAggregation],
                       spark: SparkSession
                      ): DataFrame = {

    import spark.implicits._

    val token: String = getParams match {
      case Some(p) => p.head
      case _ => throw new RuntimeException(s"No field specified")
    }

    val rptDs = token match {
      case "dataProvider" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(Seq(dplaMapData.dataProvider))))
      case "edmRights" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.edmRights.toSeq)))
      case "isShownAt" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(Seq(dplaMapData.isShownAt.uri))))
      case "sourceResource.alternateTitle" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.alternateTitle)))
      case "sourceResource.collection.title" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.collection)))
      case "sourceResource.contributor.name" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.contributor)))
      case "sourceResource.creator.name" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.creator)))
      case "sourceResource.date.originalSourceDate" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.date)))
      case "sourceResource.description" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.description)))
      case "sourceResource.extent" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.extent)))
      case "sourceResource.format" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(value = extractValue(dplaMapData.sourceResource.format)))
      case "sourceResource.genre" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.genre)))
      case "sourceResource.identifier" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.identifier)))
      case "sourceResource.language.providedLabel" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.language)))
      case "sourceResource.place.name" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.place)))
      case "sourceResource.publisher.name" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.publisher)))
      case "sourceResource.relation" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.relation)))
      case "sourceResource.replacedBy" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.replacedBy)))
      case "sourceResource.replaces" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.replaces)))
      case "sourceResource.rights" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.rights)))
      case "sourceResource.rightsHolder.name" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.rightsHolder)))
      case "sourceResource.subject.providedLabel" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.subject)))
      case "sourceResource.temporal.originalSourceDate" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.temporal)))
      case "sourceResource.title" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.title)))
      case "sourceResource.type" =>
        ds.map(dplaMapData => PropertyDistinctValueRpt(extractValue(dplaMapData.sourceResource.`type`)))
      case x =>
        throw new RuntimeException(s"Unrecognized field name '$x'")
    }

    makeTable(rptDs, spark, token)
  }


  /**
    * Explodes the value column so that multiple values
    * occur on separate rows. Then the groupBy and count
    * operations are performed on the data
    *
    * @param rptDataset Dataset to report against
    * @param spark Spark session
    * @param token Column name
    * @return
    */
  def makeTable(rptDataset: Dataset[PropertyDistinctValueRpt],
                spark: SparkSession,
                token: String): DataFrame = {
    val sqlContext = spark.sqlContext
    // Periods are not allowed in column names...
    val colName = token.replace(".", "_")
    rptDataset.createOrReplaceTempView("tmpPropValRpt")

    sqlContext.sql("""SELECT * FROM tmpPropValRpt""")
      .withColumn(colName, explode(col("value")))
       .drop(col("value"))
       .groupBy(colName)
       .count()
       .orderBy(colName, "count")
  }
}
