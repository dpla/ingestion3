package dpla.ingestion3.executors

import java.time.LocalDateTime

import com.databricks.spark.avro._
import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.model
import dpla.ingestion3.model._
import dpla.ingestion3.wiki.{WikiCriteria, WikiMapper}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.{Failure, Success, Try}

case class WikimediaMetadata(dplaId: String, wikiMarkup: String)

trait WikimediaMetadataExecutor extends Serializable with WikiMapper {

  /**
    * Generate Wiki metadata JSON files from AVRO file
    * @param sparkConf  Spark configuration
    * @param dataIn     Data to transform into Wiki metadata
    * @param dataOut    Location to save Wikimedia metadata
    * @param shortName  Provider shortname
    * @param logger     Logger object
    */
  def executeWikimediaMetadata(sparkConf: SparkConf,
                               dataIn: String,
                               dataOut: String,
                               shortName: String,
                               logger: Logger): String = {

    // This start time is used for documentation and output file naming.
    val startDateTime: LocalDateTime = LocalDateTime.now

    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "wiki", startDateTime)

    val outputPath: String = outputHelper.activityPath

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
    val tupleRowBooleanEncoder: ExpressionEncoder[(Row, Boolean)] =
      ExpressionEncoder.tuple(RowEncoder(model.sparkSchema), ExpressionEncoder())

    val enrichedRows: DataFrame = spark.read.avro(dataIn)

    val enrichResults: Dataset[(Row, Boolean)] = enrichedRows.map(row => {
      Try{ ModelConverter.toModel(row) } match {
        case Success(dplaMapData) =>
          val criteria: WikiCriteria = isWikiEligible(dplaMapData)

          (criteria.dataProvider, criteria.asset, criteria.rights, criteria.id) match {
            // All required properties exist
            case (true, true, true, true) => (RowConverter.toRow(dplaMapData, model.sparkSchema), true)
            // Not eligible
            case (_, _, _, _) => (RowConverter.toRow(dplaMapData, model.sparkSchema), false)
          }
        case Failure(_) => (null, false)
      }
    })(tupleRowBooleanEncoder)

    // Filter out only the wiki eligible records
    // TODO There should be a better way to combine these two blocks
    import spark.implicits._
    val wikiRecords: Dataset[(String, String)] = enrichResults
      .filter(tuple => tuple._2)
      .map(tuple => {
        val record = ModelConverter.toModel(tuple._1)
        val dplaId = getDplaId(record)
        val wiki = wikiRecord(record)

        (dplaId, wiki)
      })

    wikiRecords
      .write
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

