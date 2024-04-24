package dpla.ingestion3.executors

import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.model
import dpla.ingestion3.model._
import dpla.ingestion3.wiki.{WikiCriteria, WikiMapper}
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.time.LocalDateTime
import scala.util.{Failure, Success, Try}

case class WikimediaMetadata(dplaId: String, wikiMarkup: String)

trait WikimediaMetadataExecutor extends Serializable with WikiMapper {

  /** Generate Wiki metadata JSON files from AVRO file
    * @param sparkConf
    *   Spark configuration
    * @param dataIn
    *   Data to transform into Wiki metadata
    * @param dataOut
    *   Location to save Wikimedia metadata
    * @param shortName
    *   Provider shortname
    */
  def executeWikimediaMetadata(
      sparkConf: SparkConf,
      dataIn: String,
      dataOut: String,
      shortName: String
  ): String = {

    // This start time is used for documentation and output file naming.
    val startDateTime: LocalDateTime = LocalDateTime.now

    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "wiki", startDateTime)

    val outputPath: String = outputHelper.activityPath

    val logger = LogManager.getLogger(this.getClass)
    logger.info("Starting Wikimedia export")
    logger.info(s"dataIn  > $dataIn")
    logger.info(s"dataOut > $outputPath")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._

    val aSeq = allowedIds.toSeq
    val enrichedRows =
      spark.read.format("avro").load(dataIn).filter($"dplaUri".isin(aSeq: _*))

    val enrichResults = enrichedRows.rdd.map(row => {
      Try { ModelConverter.toModel(row) } match {
        case Success(dplaMapData) =>
          // If there is neither a IIIF manifest or media master mapped from the original record then try to construct
          // a IIIF manifest from the isShownAt value. This should only work for ContentDM URLs.
          val dplaMapRecord =
            if (
              dplaMapData.iiifManifest.isEmpty && dplaMapData.mediaMaster.isEmpty
            ) {
              dplaMapData.copy(iiifManifest =
                buildIIIFFromUrl(dplaMapData.isShownAt)
              )
            } else
              dplaMapData

          // evaluate the record for Wikimedia eligibility
          val criteria: WikiCriteria = isWikiEligible(dplaMapRecord)

          (
            criteria.dataProvider,
            criteria.asset,
            criteria.rights,
            criteria.id
          ) match {
            // All required properties exist
            case (true, true, true, true) =>
              (RowConverter.toRow(dplaMapRecord, model.sparkSchema), true)
            case (_, _, _, _) =>
              (RowConverter.toRow(dplaMapRecord, model.sparkSchema), false)
          }
        case Failure(_) => (null, false)
      }
    })

    println(enrichResults.count())

    // TODO There should be a better way to combine these two blocks
    // Parquet schema
    // - dplaId
    // - wikiMarkup
    // - iiifManifest
    // - mediaMaster: Seq[String]
    // - title [first value only]
    val wikiRecords: Dataset[(String, String, String, Seq[String], String)] =
      enrichResults
        // Filter out only the wiki eligible records
        .filter(tuple => tuple._2)
        .map(tuple => {
          val record = ModelConverter.toModel(tuple._1)
          val dplaId = getDplaId(record)
          val wikiMetadata = buildWikiMarkup(record)
          val iiif = record.iiifManifest.getOrElse("").toString
          val mediaMaster = record.mediaMaster.map(_.uri.toString)
          val title = record.sourceResource.title
          (dplaId, wikiMetadata, iiif, mediaMaster, title.head)
        })
        .toDS()

    wikiRecords.write
      .parquet(outputPath)

    // Create and write manifest.
    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "Wiki",
      "Provider" -> shortName,
      "Record count" -> s"${wikiRecords.count}",
      "Input" -> dataIn
    )

    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    sc.stop()

    logger.info("Wikimedia export complete")

    // Return output path of wiki files.
    outputPath
  }
}
