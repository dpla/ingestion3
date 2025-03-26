import dpla.ingestion3.model.{ModelConverter, jsonlRecord}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.json4s.jackson.JsonMethods._

import org.json4s._
import org.json4s.jackson.JsonMethods.compact


val baseConf = new SparkConf()
  .setAppName(s"nara filecoin")
  .setMaster("local[*]")

implicit val spark: SparkSession = SparkSession
  .builder()
  .config(baseConf)
  .config("spark.ui.showConsoleProgress", value = false)
  .getOrCreate()

import spark.implicits._
val cd = "/Users/michael/dpla/ingestion3"

case class Folder(id: String, url: String)

val folders = spark.read.format("csv")
  .option("header", "true")
  .load(cd + "/src/main/resources/filecoin-nara/")
  .drop("number of files", "file size")
  .withColumnRenamed("Folder Name", "id")
  .as[Folder]

case class NaraWithId(id: String, json: String)

val nara = spark.read.format("avro")
  .load("/Users/michael/dpla/data/nara/enrichment/20250227_154719-nara-MAP4_0.EnrichRecord.avro")
  .map(row => {
    val model = ModelConverter.toModel(row)
    val id = model.dplaUri.toString.split("/").last
    val json = jsonlRecord(model)
    NaraWithId(id, json)
  }).as[NaraWithId]


val results = nara
  .join(folders, nara("id") === folders("id"))
  .select("json", "URL")
  .filter("URL is not null")
  .map(row => {
    val json = row.getString(0)
    val url = row.getString(1)
    val parsed = parse(json)
    val enhanced = parsed merge JObject("_source" -> JObject("ipfs" -> JString(url)))
    compact(render(enhanced))
  })

results.write.mode("overwrite").option("compression", "gzip").text("/Users/michael/with-ipfs-urls.jsonl")



//
//val urls = nara.map(row => {
//  val oreAggregation = ModelConverter.toModel(row)
//
//})

