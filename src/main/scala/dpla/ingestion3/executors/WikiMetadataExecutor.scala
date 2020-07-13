package dpla.ingestion3.executors

import java.io.File
import java.time.LocalDateTime

import com.databricks.spark.avro._
import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.model
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import dpla.ingestion3.wiki.{WikiCriteria, WikiMapper}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

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
    logger.info(s"dataIn  > $dataIn")
    logger.info(s"dataOut > $outputPath")

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

    val enrichResults: Dataset[(Row, Boolean)] = enrichedRows.map(row => {
      Try{ ModelConverter.toModel(row) } match {
        case Success(dplaMapData) =>
          val criteria: WikiCriteria = isWikiEligible(dplaMapData)

          (criteria.dataProvider, criteria.asset, criteria.rights, criteria.id) match {
            // All required properties exist
            case (true, true, true, true) => (RowConverter.toRow(dplaMapData, model.sparkSchema), true)
            // All other cases
            case (_, _, _, _) => (RowConverter.toRow(dplaMapData, model.sparkSchema), false)
          }
        case Failure(_) => (null, false)
      }
    })(tupleRowBooleanEncoder)

    // Filter out only the wiki eligible records
    val wikiRecords = enrichResults
      .filter(tuple => tuple._2)
      .map(tuple => tuple._1)(dplaMapDataRowEncoder)

    wikiRecords.foreach(row => {
      val record = ModelConverter.toModel(row)
      val dplaId = getDplaId(record)
      val wikiMarkup = wikiRecord(record)
      val itemPath = getWikiPath(dplaId)

      import org.json4s.jackson.JsonMethods._
      val wikiJson = pretty(render(parse(wikiMarkup))(formats))

      val wikiMetadata =
        s"""
           | "markup": $wikiJson
        """.stripMargin

      writeOut(s"$outputPath/$itemPath", wikiMetadata)
    })
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

    logger.info("Wiki export complete")

    // Return output path of wiki files.
    outputPath
  }

  /**
    * Construct path from DPLA id
    *
    * @param id DPLA id
    * @return
    */
  def getWikiPath(id: String): String = {
    s"${id(0)}/${id(1)}/${id(2)}/${id(3)}/$id/"
  }

  /**
    * Write metadata string out to file
    *
    * @param path Directory to write output to
    * @param metadata Content to write to file
    * @return
    */
  def writeOut(path: String, metadata: String) = {
    new File(path).mkdirs()
    new FlatFileIO().writeFile(metadata, s"$path/metadata.json")
  }

}

