package dpla.ingestion3.entries.utils

import dpla.ingestion3.confs.CmdArgs
import dpla.ingestion3.dataStorage.{InputHelper, OutputHelper}
import dpla.ingestion3.model.{OreAggregation, jsonlRecord}
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.time.LocalDateTime
import scala.util.{Failure, Success}

object NaraFilecoin {

  private val logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val cmdArgs = new CmdArgs(args)
    val baseDataOut = cmdArgs.getOutput
    val shortName = cmdArgs.getProviderName
    val input = cmdArgs.getInput
    val sparkMaster: Option[String] = cmdArgs.getSparkMaster

    val inputDirectory =
      if (InputHelper.isActivityPath(input)) input
      else
        InputHelper
          .mostRecent(input)
          .getOrElse(throw new RuntimeException("Unable to load enriched data."))

    val startDateTime: LocalDateTime = LocalDateTime.now
    val outputHelper = new OutputHelper(baseDataOut, shortName, "jsonl", startDateTime)
    val outputPath = outputHelper.activityPath

    val baseConf = new SparkConf()
      .setAppName(s"NARA Filecoin CID Merge")

    val sparkConf = sparkMaster match {
      case Some(m) => baseConf.setMaster(m)
      case None    => baseConf
    }

    implicit val spark: SparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .config("spark.ui.showConsoleProgress", value = false)
      .getOrCreate()

    import spark.implicits._

    implicit val oreAggregationEncoder: ExpressionEncoder[OreAggregation] =
      ExpressionEncoder[OreAggregation]

    case class Folder(id: String, url: String)
    implicit val folderEncoder: ExpressionEncoder[Folder] = ExpressionEncoder[Folder]

    val folders = spark.read
      .format("csv")
      .option("header", "true")
      .load("src/main/resources/filecoin-nara/")
      .drop("number of files", "file size")
      .withColumnRenamed("Folder Name", "id")
      .as[Folder]

    case class NaraWithId(id: String, json: String)
    implicit val naraWithIdEncoder: ExpressionEncoder[NaraWithId] =
      ExpressionEncoder[NaraWithId]

    val nara = spark.read
      .format("avro")
      .load(inputDirectory)
      .as[OreAggregation]
      .map(row => {
        val id = row.dplaUri.toString.split("/").last
        val json = jsonlRecord(row)
        NaraWithId(id, json)
      })

    val results = nara
      .join(folders, nara("id") === folders("id"))
      .select("json", "URL")
      .filter("URL is not null")
      .map(row => {
        val json = row.getString(0)
        val url = row.getString(1)
        val parsed = parse(json)
        val enhanced =
          parsed merge JObject("_source" -> JObject("ipfs" -> JString(url)))
        compact(render(enhanced))
      })

    results.write
      .mode("overwrite")
      .option("compression", "gzip")
      .text(outputPath)

    val indexCount = spark.read.text(outputPath).count()

    // Create and write manifest.
    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "JSON-L",
      "Provider" -> "nara",
      "Record count" -> indexCount.toString,
      "Input" -> inputDirectory
    )

    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    spark.stop()

    logger.info("NARA Filecoin JSON-L export complete")
  }
}
