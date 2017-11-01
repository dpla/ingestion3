package dpla.ingestion3.reports
import dpla.ingestion3.model._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class PropertyValueRpt(dplaUri: String,
                            localUri: String,
                            value: Seq[String])

class PropertyValueReport (
                            val inputURI: String,
                            val outputURI: String,
                            val sparkConf: SparkConf,
                            val params: Array[String]) extends Report with Serializable {

  override val sparkAppName: String = "PropertyValueReport"
  override def getInputURI: String = inputURI
  override def getOutputURI: String = outputURI
  override def getSparkConf: SparkConf = sparkConf
  override def getParams: Option[Array[String]] = {
    if (params.nonEmpty) {
      Some(params)
    } else {
      None
    }
  }
  val dplaUriCol = "dpla uri"
  val localUriCol = "local uri"

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
  override def process(ds: Dataset[OreAggregation], spark: SparkSession): DataFrame = {
    import spark.implicits._

    val token: String = getParams match {
      case Some(p) => p.head
      case _ => throw new RuntimeException(s"No field specified")
    }

    implicit val dplaMapDataEncoder =
      org.apache.spark.sql.Encoders.kryo[OreAggregation]

    val rptDataset: Dataset[PropertyValueRpt] = token match {
      case "edmRights" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.edmRights.toSeq)
          )
        })
      case "sourceResource.alternateTitle" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.alternateTitle)
          )
        })
      case "sourceResource.contributor.name" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.contributor)
          )
        })
      case "sourceResource.collection.title" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.collection)
          )
        })
      case "sourceResource.creator.name" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.creator)
          )
        })
      case "sourceResource.date.originalSourceDate" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.date)
          )
        })
      case "sourceResource.description" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.description)
          )
        })
      case "sourceResource.extent" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.extent)
          )
        })
      case "sourceResource.format" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.format)
          )
        })
      case "sourceResource.genre" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.genre)
          )
        })
      case "sourceResource.identifier" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.identifier)
          )
        })
      case "sourceResource.language.providedLabel" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.language)
          )
        })
      case "sourceResource.place.name" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.place)
          )
        })
      case "sourceResource.publisher.name" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.publisher)
          )
        })
      case "sourceResource.relation" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.relation)
          )
        })
      case "sourceResource.replacedBy" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.replacedBy)
          )
        })
      case "sourceResource.replaces" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.replaces)
          )
        })
      case "sourceResource.rights" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.rights)
          )
        })
      case "sourceResource.rightsHolder.name" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.rightsHolder)
          )
        })
      case "sourceResource.subject.providedLabel" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.subject)
          )
        })
      case "sourceResource.temporal.originalSourceDate" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.temporal)
          )
        })
      case "sourceResource.title" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.title)
          )
        })
      case "sourceResource.type" =>
        ds.map(oreAggregation => {
          PropertyValueRpt(
            dplaUri = oreAggregation.dplaUri.toString,
            localUri = oreAggregation.isShownAt.uri.toString,
            value = extractValue(oreAggregation.sourceResource.`type`)
          )
        })
      case x =>
        throw new RuntimeException(s"Unrecognized field name '$x'")
    }

    makeTable(rptDataset, spark, token)
  }

  /**
    * Responsible for exploding the value column of the
    * DataFrame so that multiple values occur on separate
    * rows.
    *
    * @param rptDataset Report dataset
    * @param spark Spark Session
    * @param token The name of the column being reported on
    * @return
    */
  def makeTable(rptDataset: Dataset[PropertyValueRpt],
                spark: SparkSession,
                token: String): DataFrame = {
    val sqlContext = spark.sqlContext
    rptDataset.createOrReplaceTempView("tmpPropValRpt")

    sqlContext.sql("""SELECT * FROM tmpPropValRpt""")
      .withColumn(token, explode(col("value")))
      .drop(col("value"))
  }
}