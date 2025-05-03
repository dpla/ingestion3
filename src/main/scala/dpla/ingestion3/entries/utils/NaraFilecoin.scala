package dpla.ingestion3.entries.utils

import dpla.ingestion3.model.{ModelConverter, jsonlRecord}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._

object NaraFilecoin {

  private case class Folder(id: String, url: String)

  def main(args: Array[String]): Unit = {

    val baseConf = new SparkConf()
      .setAppName(s"NARA Filecoin CID Merge")

    implicit val spark: SparkSession = SparkSession
      .builder()
      .config(baseConf)
      .config("spark.ui.showConsoleProgress", value = false)
      .getOrCreate()

    import spark.implicits._

    val folders = spark.read
      .format("csv")
      .option("header", "true")
      .load("src/main/resources/filecoin-nara/")
      .drop("number of files", "file size")
      .withColumnRenamed("Folder Name", "id")
      .as[Folder]

    case class NaraWithId(id: String, json: String)

    val nara = spark.read
      .format("avro")
      .load("src/main/resources/nara/")
      .map(row => {
        val model = ModelConverter.toModel(row)
        val id = model.dplaUri.toString.split("/").last
        val json = jsonlRecord(model)
        NaraWithId(id, json)
      })
      .as[NaraWithId]

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
      .text("/Users/michael/with-ipfs-urls.jsonl")
  }
}
