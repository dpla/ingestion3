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
import org.apache.spark.storage.StorageLevel

import scala.util.{Failure, Success, Try}

case class WikiMetadata(dplaId: String, wikiMarkup: String)

trait WikiMetadataExecutor extends Serializable with WikiMapper {

  /**
    * Generate Wiki metadata JSON files from AVRO file
    * @param sparkConf  Spark configuration
    * @param dataIn     Data to transform into Wiki metadata
    * @param dataOut    Location to save Wikimedia metadata
    * @param shortName  Provider shortname
    * @param logger     Logger object
    */
  def executeWikiMetadata(sparkConf: SparkConf,
                   dataIn: String,
                   dataOut: String,
                   shortName: String,
                   logger: Logger): String = {

    // This start time is used for documentation and output file naming.
    val startDateTime: LocalDateTime = LocalDateTime.now

    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "wiki", startDateTime)

    val outputPath: String = outputHelper.activityPath

    logger.info("Starting Wiki export")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sc = spark.sparkContext

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._
    val dplaMapDataRowEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)
    val tupleRowBooleanEncoder: ExpressionEncoder[(Row, Boolean)] =
      ExpressionEncoder.tuple(RowEncoder(model.sparkSchema), ExpressionEncoder())

    val enrichedRows: DataFrame = spark.read.avro(dataIn)

    println("enriched count " + enrichedRows.count())

    val enrichResults: Dataset[(Row, Boolean)] = enrichedRows.map(row => {
      Try{ ModelConverter.toModel(row) } match {
        case Success(dplaMapData) =>

          /**
            * FIXUP
            *
            *  - Change from print to message logger
            *
            */
          val criteria: WikiCriteria = isWikiEligible(dplaMapData)
          (criteria.dataProvider, criteria.asset, criteria.rights) match {
            // All required properties exist
            case (true, true, true) => (RowConverter.toRow(dplaMapData, model.sparkSchema), true)
            // Missing valid standardized rights
            case (true, true, false) =>
              println(s"${dplaMapData.dplaUri.toString} is missing valid rights. Value is ${dplaMapData.edmRights.getOrElse("_MISSING_")}")
              (null, false)
            // Missing assets
            case (true, false, true) =>
              println(s"${dplaMapData.dplaUri.toString} is missing assets.")
              (null, false)
            // Missing dataProvider URI
            case (false, true, true) =>
              println(s"${dplaMapData.dplaUri.toString} is missing dataProvider URI. Value is ${dplaMapData.dataProvider.name.getOrElse("_MISSSING_")}")
              (null, false)
            // Multiple missing required properties
            case (_, _, _) =>
              println(s"${dplaMapData.dplaUri.toString} is missing multiple requirements. " +
                s"\n- dataProvider missing URI. Value is ${dplaMapData.dataProvider.name.getOrElse("_MISSSING_")}" +
                s"\n- edmRights is ${dplaMapData.edmRights.getOrElse("_MISSING_")}" +
                s"\n- iiif is ${dplaMapData.iiifManifest.getOrElse("_MISSING_")}" +
                s"\n- mediaMaster is ${dplaMapData.mediaMaster.map(_.uri.toString).mkString("; ").orElse("_MISSING_")}")
              (null, false)
          }
        case Failure(err) => (null, false)
      }
    })(tupleRowBooleanEncoder)

    println(s"enrichedResults count = ${enrichResults.count()}")

    // Filter out only the wiki eligible records
    val wikiRecords = enrichResults
      .filter(tuple => tuple._2)
      .map(tuple => tuple._1)(dplaMapDataRowEncoder)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // Fixup to logger
    println(s"wiki records count ${wikiRecords.count()}")

    val wikiRows: Dataset[String] = wikiRecords.map(
      row => {
        val record = ModelConverter.toModel(row)
        val dplaId = getDplaId(record)
        val wikiMarkup = wikiRecord(record)
        val path = getWikiPath(dplaId)
        // val assets = getWikiAssets(record)


        import org.json4s.jackson.JsonMethods._
        val wikiJson = pretty(render(parse(wikiMarkup))(formats))



        val string =
          s"""
            |
            | Output path: $path
            | Output file: TBD
            | Wikimarkup: ${wikiJson}
            |
          """.stripMargin

        string
        // TODO write files out
      }
    ).persist(StorageLevel.MEMORY_AND_DISK_SER)

    wikiRows.take(100).foreach(println)

    // Create and write manifest.

    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "Wiki",
      "Provider" -> shortName,
      "Record count" -> "TBD",
      "Input" -> dataIn
    )

    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    sc.stop()

    logger.info("Wiki export complete")

    // Return output path of wiki files.
    outputPath
  }

  /**
    *
    * @param str
    * @return
    */
  def getWikiPath(str: String) = {
    s"{${str(0)}/${str(1)}/${str(2)}/${str(3)}/$str/}"
  }


}

