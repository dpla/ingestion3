package dpla.ingestion3.executors

import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.model._
import dpla.ingestion3.wiki.WikiMapper
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.LocalDateTime
import scala.util.{Failure, Success}

trait WikimediaMetadataExecutor extends Serializable with WikiMapper {

  /** Generate wiki parquet files from an enriched AVRO file.
    *
    * @param sparkConf
    *   Spark configuration
    * @param dataIn
    *   Path to enriched AVRO data
    * @param dataOut
    *   Root output path for wiki parquet
    * @param shortName
    *   Provider short name (e.g. "pa", "bpl")
    */
  def executeWikimediaMetadata(
      sparkConf: SparkConf,
      dataIn: String,
      dataOut: String,
      shortName: String
  ): String = {

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

    import spark.implicits._

    val enrichedRows: Dataset[OreAggregation] =
      spark.read.format("avro").load(dataIn).as[OreAggregation]

    // Parquet schema: dplaId, wikiMarkup, iiifManifest, mediaMaster, title
    val wikiRecords: Dataset[(String, String, String, Seq[String], String)] =
      enrichedRows
        .map { record =>
          // If no IIIF manifest or media master, try to derive IIIF from isShownAt
          // (works for ContentDM URLs).
          if (record.iiifManifest.isEmpty && record.mediaMaster.isEmpty)
            record.copy(iiifManifest = buildIIIFFromUrl(record.isShownAt))
          else
            record
        }
        .filter { record =>
          val criteria = isWikiEligible(record)
          criteria.dataProvider && criteria.asset && criteria.rights && criteria.id
        }
        .map { record =>
          val dplaId = record.dplaUri.toString.split("/").last
          val markup = buildWikiMarkup(record)
          val iiif   = record.iiifManifest.getOrElse("").toString
          val media  = record.mediaMaster.map(_.uri.toString)
          val title  = record.sourceResource.title.headOption.getOrElse("")
          (dplaId, markup, iiif, media, title)
        }

    val cachedRecords = wikiRecords.cache()
    cachedRecords.write.parquet(outputPath)
    val recordCount = cachedRecords.count()
    cachedRecords.unpersist()

    val manifestOpts: Map[String, String] = Map(
      "Activity"     -> "Wiki",
      "Provider"     -> shortName,
      "Record count" -> recordCount.toString,
      "Input"        -> dataIn
    )

    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    spark.stop()

    logger.info("Wikimedia export complete")

    outputPath
  }
}
