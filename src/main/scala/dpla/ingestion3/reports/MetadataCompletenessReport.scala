package dpla.ingestion3.reports

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import dpla.ingestion3.model._
import org.apache.spark.sql.functions.mean

/** Metadata Completeness QA report.
  */
class MetadataCompletenessReport(
    val input: Dataset[OreAggregation],
    val spark: SparkSession,
    val params: Array[String] = Array()
) extends Report
    with Serializable {

  override val sparkAppName: String = "MetadataCompletenessReport"
  override def getInput: Dataset[OreAggregation] = input
  override def getSparkSession: SparkSession = spark
  override def getParams: Option[Array[String]] = {
    if (params.nonEmpty) Some(params) else None
  }

  /** Process the incoming dataset.
    *
    * @see
    *   Report.process()
    *
    * @param ds
    *   Dataset of DplaMapData (mapped or enriched records)
    * @param spark
    *   The Spark session, which contains encoding / parsing info.
    * @return
    *   DataFrame, typically of Row[value: String, count: Int]
    */
  override def process(
      ds: Dataset[OreAggregation],
      spark: SparkSession
  ): DataFrame = {

    val sqlContext = spark.sqlContext

    val itemTallies: Dataset[CompletenessTally] = getItemTallies(ds, spark)
    itemTallies.createOrReplaceTempView("itemTallies")

    // For each record, get the metadata completeness "score" for each metric.
    val metrics = sqlContext.sql("""select title + rights + dataProvider +
                                        isShownAt + provider + preview
                                        as mandatory,
                                      title + description + creator + language +
                                        subject + extent + format
                                        as descriptiveness,
                                      title + description + creator +
                                        publisher + contributor + type +
                                        place + subject + date + relation
                                        as searchability,
                                      collection + description + creator +
                                        type + date + place + subject + relation
                                        as contextualization,
                                      title + collection + description +
                                        creator + type + identifier + date
                                        as identification,
                                      title + creator + type + place +
                                        subject + date + relation
                                        as browsing,
                                      creator + publisher + date + rights +
                                        isShownAt
                                        as reusability,
                                      title + description + creator + type +
                                        identifier + language + place + subject +
                                        date + format + rights + dataProvider +
                                        isShownAt + provider + preview
                                        as completeness
                                      from itemTallies""")

    // Get the average metric scores across the data sample.
    metrics.agg(
      mean("mandatory").alias("mandatory"),
      mean("descriptiveness").alias("descriptiveness"),
      mean("searchability").alias("searchability"),
      mean("contextualization").alias("contextualization"),
      mean("identification").alias("identification"),
      mean("browsing").alias("browsing"),
      mean("reusability").alias("reusability"),
      mean("completeness").alias("completeness")
    )
  }

  /** Map a Dataset of DplaMapData to a Dataset of CompletenessTally.
    */
  private def getItemTallies(
      ds: Dataset[OreAggregation],
      spark: SparkSession
  ): Dataset[CompletenessTally] = {

    import spark.implicits._

    ds.map(dplaMapData => {

      CompletenessTally(
        title = tally(dplaMapData.sourceResource.title),
        collection = tally(dplaMapData.sourceResource.title),
        description = tally(dplaMapData.sourceResource.description),
        creator = tally(dplaMapData.sourceResource.creator),
        publisher = tally(dplaMapData.sourceResource.publisher),
        contributor = tally(dplaMapData.sourceResource.creator),
        `type` = tally(dplaMapData.sourceResource.`type`),
        identifier = tally(dplaMapData.sourceResource.identifier),
        language = tally(dplaMapData.sourceResource.language),
        temporal = tally(dplaMapData.sourceResource.temporal),
        place = tally(dplaMapData.sourceResource.place),
        subject = tally(dplaMapData.sourceResource.subject),
        date = tally(dplaMapData.sourceResource.date),
        extent = tally(dplaMapData.sourceResource.extent),
        format = tally(dplaMapData.sourceResource.format),
        relation = tally(dplaMapData.sourceResource.relation),
        id = tally(Seq(dplaMapData.dplaUri)),
        dataProvider = tally(Seq(dplaMapData.dataProvider)),
        provider = tally(Seq(dplaMapData.provider)),
        preview = tally(Seq(dplaMapData.preview)),
        rights = {
          val sourceResourceRights = dplaMapData.sourceResource.rights
          val edmRights = dplaMapData.edmRights
          if (sourceResourceRights.nonEmpty || edmRights.nonEmpty) 1 else 0
        },
        // add isShownAt value once it has been added to DplaMapData
        isShownAt = 0
      )
    })
  }

  /** Get an integer representing whether or not a value is present.
    *
    * @param value:
    *   A Sequence containing zero to many values from a DplaDataMap object.
    * @return
    *   1 if there is at least one value; otherwise 0
    */
  private def tally(value: Seq[Any]): Integer = {
    if (value.nonEmpty) 1 else 0
  }
}

/** Tallies the presence or absence of values in certain fields for a single
  * record; 1 if the field value is present; 0 if the field value is absent.
  * Numerical representations are used b/c it makes it easier to calculate
  * totals, averages, etc.
  */
case class CompletenessTally(
    title: Integer,
    collection: Integer,
    description: Integer,
    creator: Integer,
    publisher: Integer,
    contributor: Integer,
    `type`: Integer,
    identifier: Integer,
    language: Integer,
    temporal: Integer,
    place: Integer,
    subject: Integer,
    date: Integer,
    extent: Integer,
    format: Integer,
    relation: Integer,
    id: Integer,
    dataProvider: Integer,
    provider: Integer,
    preview: Integer,
    isShownAt: Integer,
    rights: Integer
)
