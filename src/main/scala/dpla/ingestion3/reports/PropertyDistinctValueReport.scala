package dpla.ingestion3.reports

import org.apache.spark.sql.{DataFrame, Row, SparkSession, Dataset}
import dpla.ingestion3.model._

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
  * @param sparkMasterName  Spark master name, e.g. "local[1]"
  * @param params           Additional parameters, currently:
  *                         params(0): The DPLA MAP field to analyze
  */
class PropertyDistinctValueReport(
                            val inputURI: String,
                            val outputURI: String,
                            val sparkMasterName: String,
                            val params: Array[String]
                          ) extends Report {

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
  override def getSparkMasterName: String = sparkMasterName
  override def getParams: Option[Array[String]] = {
    if (params.nonEmpty) Some(params) else None
  }


  /**
    * Process the incoming dataset.
    *
    * @see          Report.process()
    *
    * @param ds     Dataset of DplaMapData (mapped or enriched records)
    * @param spark  The Spark session, which contains encoding / parsing info.
    * @return       DataFrame, typically of Row[value: String, count: Int]
    */
  override def process(ds: Dataset[DplaMapData],
                       spark: SparkSession
                      ): DataFrame = {

    import spark.implicits._

    val token: String = getParams match {
      case Some(p) => p.head
      case _ => throw new RuntimeException(s"No field specified")
    }

    token match {
        /*
         * FIXME: "java.lang.UnsupportedOperationException: No Encoder found
         * for java.net.URI" for sourceResource.language, subject, and other
         * fields of classes that have URIs, even if you're not evaluating
         * one of the URI fields in that dpla.ingestion3.model case class.
         */
      case "sourceResource.format" =>
        ds.map(dplaMapData => dplaMapData.sourceResource.format)
          .flatMap(x => x)
          .groupBy("value")
          .count
      case "sourceResource.rights" =>
        ds.map(dplaMapData => dplaMapData.sourceResource.rights)
          .flatMap(x => x)
          .groupBy("value")
          .count
      case "sourceResource.type" =>
        ds.map(dplaMapData => dplaMapData.sourceResource.`type`)
          .flatMap(x => x)
          .groupBy("value")
          .count
      case x =>
        throw new RuntimeException(s"Unrecognized field name '$x'")
    }

  }

}
