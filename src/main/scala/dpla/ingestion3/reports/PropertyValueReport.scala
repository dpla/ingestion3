package dpla.ingestion3.reports
import dpla.ingestion3.model.DplaMapData
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class PropertyValueReport (
                            val inputURI: String,
                            val outputURI: String,
                            val sparkMasterName: String,
                            val params: Array[String]) extends Report {

  override val sparkAppName: String = "PropertyValueReport"
  override def getInputURI: String = inputURI
  override def getOutputURI: String = outputURI
  override def getSparkMasterName: String = sparkMasterName
  override def getParams: Option[Array[String]] = {
    params.nonEmpty match {
      case true => Some(params)
      case _ => None
    }
  }

  def splitOnPipe(str: String) = str.split("|")

  /**
    * Process the incoming dataset (mapped or enriched records) and return a
    * DataFrame of computed results.
    *
    * This report returns:
    *   local uri, dpla uri, value
    *
    * If value is an array then multiple values are sent to separate rows.
    * E.x.
    *   id1, format1, dplaId1
    *   id1, format2, dplaId1
    *
    * Overridden by classes in dpla.ingestion3.reports
    *
    * @param ds    Dataset of DplaMapData (mapped or enriched records)
    * @param spark The Spark session, which contains encoding / parsing info.
    * @return DataFrame, typically of Row[value: String, count: Int]
    */
  override def process(ds: Dataset[DplaMapData], spark: SparkSession): DataFrame = {
    import spark.implicits._

    val token: String = getParams match {
      case Some(p) => p.head
      case _ => throw new RuntimeException(s"No field specified")
    }

    implicit val dplaMapDataEncoder =
      org.apache.spark.sql.Encoders.kryo[DplaMapData]

    token match {
        // TODO Is there a cleaner way of including the local and dpla uris
        // so they aren't repeated for every single case block?
      case "sourceResource.format" =>
          ds.map(dplaMapData => ( dplaMapData.edmWebResource.uri.toString,
                                dplaMapData.sourceResource.format,
                                dplaMapData.oreAggregation.uri.toString))
            .withColumnRenamed("_1", "local uri")
            .withColumnRenamed("_3", "dpla uri")
            .withColumn("format", org.apache.spark.sql.functions.explode($"_2"))
            .drop("_2")

      case "sourceResource.rights" =>
        ds.map(dplaMapData => ( dplaMapData.edmWebResource.uri.toString,
                                dplaMapData.sourceResource.rights,
                                dplaMapData.oreAggregation.uri.toString))
          .withColumnRenamed("_1", "local uri")
          .withColumnRenamed("_3", "dpla uri")
          .withColumn("rights", org.apache.spark.sql.functions.explode($"_2"))
          .drop("_2")

      case "sourceResource.type" =>
        ds.map(dplaMapData => ( dplaMapData.edmWebResource.uri.toString,
                                dplaMapData.sourceResource.`type`,
                                dplaMapData.oreAggregation.uri.toString))
          .withColumnRenamed("_1", "local uri")
          .withColumnRenamed("_3", "dpla uri")
          .withColumn("type", org.apache.spark.sql.functions.explode($"_2"))
          .drop("_2")

      case x =>
        throw new RuntimeException(s"Unrecognized field name '${x}'")
    }
  }


}
