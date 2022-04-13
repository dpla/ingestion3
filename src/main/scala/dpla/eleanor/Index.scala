package dpla.eleanor

import dpla.eleanor.Schemata.{IndexData, MappedData}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.log4j.{LogManager, Logger}
import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization.write
import okhttp3.{MediaType, OkHttpClient, Request, RequestBody, Response}
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark.rdd.EsSpark

import java.io.InputStream
import java.util.concurrent.TimeUnit
import scala.io.{BufferedSource, Source}
import scala.util.Try

object Index {
  // This log is serializable
  @transient lazy val log: Logger = LogManager.getLogger("Index")

  def execute(spark: SparkSession, mappedData: Dataset[MappedData], esHost: String, esPort: String,
              indexName: String, shards: Int): Unit = {

    val httpClient = new OkHttpClient.Builder()
      .connectTimeout(5, TimeUnit.SECONDS)
      .retryOnConnectionFailure(false)
      .followRedirects(false)
      .build()

    createIndex(httpClient, esHost, esPort, indexName, shards)
    val indexData = writePayloads(mappedData, spark)
    writeToIndex(indexData, spark, esHost, esPort, indexName)
  }

  def createIndex(httpClient: OkHttpClient,
                  host: String,
                  port: String,
                  indexName: String,
                  shards: Int): Unit = {

    val settingFilePath: String = "/elasticsearch/index-settings-and-mapping-ebooks.json"

    val stream: InputStream = getClass.getResourceAsStream(settingFilePath)
    val bufferedSource: BufferedSource = Source.fromInputStream(stream)
    val settingsString: String = bufferedSource.getLines.mkString
    bufferedSource.close

    val settingsTemplate: JValue = parse(settingsString)

    val settings: JValue = settingsTemplate.replace(
      "settings" :: "index" :: Nil,
      ("number_of_shards" -> shards) ~ ("number_of_replicas" -> 0)
    )

    val putBody: String = compact(render(settings))
    val mt: MediaType = MediaType.parse("application/json")
    val reqBody: RequestBody = RequestBody.create(mt, putBody)

    val request: Request =
      new Request.Builder()
        .url(s"http://$host:$port/$indexName")
        .put(reqBody)
        .build()

    val response: Response = httpClient.newCall(request).execute()

    if (!response.isSuccessful) {
      throw new RuntimeException(
        s"FAILED request for ${request.url.toString} (${response.code}, " +
          s"${response.message})"
      )
    }

    response.close()
  }

  def writePayloads(mappedData: Dataset[MappedData],
                    spark: SparkSession): Dataset[IndexData] = {

    import spark.implicits._

    mappedData.map { mapped =>
      implicit val formats: DefaultFormats.type = DefaultFormats

      val payloadUris = mapped.payloads.map { payload =>
        // Write to s3
        println(s"Writing files to S3")
        S3Writer.writePayloadToS3(payload, mapped.id)
      }

      IndexData(
        id = mapped.id,

        sourceUri = mapped.sourceUri,
        itemUri = mapped.itemUri,
        providerName = mapped.providerName,
        payloadUri = payloadUris,

        title = mapped.title,
        author = mapped.author,
        subtitle = mapped.subtitle,
        language = mapped.language,
        medium = mapped.medium,
        publisher = mapped.publisher,
        publicationDate = mapped.publicationDate,
        summary = mapped.summary,
        genre = mapped.genre
      )
    }
  }

  def writeToIndex(indexData: Dataset[IndexData],
                   spark: SparkSession,
                   host: String,
                   port: String,
                   indexName: String): Unit = {

    import spark.implicits._

    val indexJson: RDD[String] = indexData.map { i =>
      implicit val formats: DefaultFormats.type = DefaultFormats
      write(i)
    }.rdd

    val configs = Map(
      "es.nodes" -> host,
      "es.port" -> port,
      "es.input.json" -> "yes",
      "es.output.json" -> "yes",
      "es.nodes.wan.only" -> "true",
      "es.mapping.id" -> "id"
    )

    Try(EsSpark.saveJsonToEs(indexJson, indexName, configs)) match {
      case util.Success(_) => Unit
      case util.Failure(e) => println(s"ERROR: ${e.getMessage}")
    }
  }
}
