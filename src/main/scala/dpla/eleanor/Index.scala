package dpla.eleanor

import dpla.eleanor.Schemata.Ebook
import dpla.ingestion3.mappers.utils.JsonExtractor
import dpla.ingestion3.model.{jsonlRecord, stringOnlyWebResource}
import okhttp3._
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, JValue}

import java.io.InputStream
import java.util.concurrent.TimeUnit
import scala.io.{BufferedSource, Source}
import scala.util.{Failure, Success, Try}

object Index extends JsonExtractor {
  // This log is serializable
  @transient lazy val log: Logger = LogManager.getLogger("Index")
  implicit val formats: DefaultFormats.type = DefaultFormats

  /**
   *
   * @param spark
   * @param mappedData
   * @param esHost
   * @param esPort
   * @param indexName
   * @param shards
   */
  def execute(spark: SparkSession, mappedData: Dataset[Ebook], esHost: String, esPort: String,
              indexName: String, shards: Int): Unit = {

    val httpClient = new OkHttpClient.Builder()
      .connectTimeout(5, TimeUnit.SECONDS)
      .retryOnConnectionFailure(false)
      .followRedirects(false)
      .build()

    // Create search index
    createIndex(httpClient, esHost, esPort, indexName, shards)
    // Write payload data to S3, add S3 URLs to dataset
    val payloadDataset = writePayloads(mappedData, spark)
    // Create indexable JSON dataset
    val indexData = createIndexDataset(payloadDataset, spark)
    // Write ebook index
    writeToIndex(indexData, spark, esHost, esPort, indexName)
  }

  /**
   *
   * @param httpClient
   * @param host
   * @param port
   * @param indexName
   * @param shards
   */
  def createIndex(httpClient: OkHttpClient,
                  host: String,
                  port: String,
                  indexName: String,
                  shards: Int): Unit = {

    val settingFilePath: String = "/elasticsearch/index-settings-and-mappings-ebook.json"

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
        .url(s"http://$host:$port/$indexName?include_type_name=true")
        .put(reqBody)
        .build()

    val response: Response = httpClient.newCall(request).execute()

    if (!response.isSuccessful) {
      throw new RuntimeException(
        s"FAILED request for ${request.url.toString} (${response.code}, ${response.message})\n${putBody}"
      )
    }

    response.close()
  }

  /**
   * Write Payloads (Array[Byte]) to S3 bucket and embeds S3 urls in the mediaMaster field
   *  - FIXME use of mediaMaster is a kludge to have asset URLs available in ES index until Payloads are
   *    added to ES schema, model etc
   *
   * @param mappedData
   * @param spark
   * @return
   */
  def writePayloads(mappedData: Dataset[Ebook],
                    spark: SparkSession): Dataset[Ebook] = {
    import spark.implicits._

    mappedData.map { ebook =>
        // FIXME highly inefficient
        // Loop over each payload entry and write the assets to S3, if it cannot be written to S3 then it should not be
        // written to the ES index
        // Get DPLA ID for record
        import org.json4s._
        val dplaId = extractString(parse(ebook.oreAggregation.sidecar) \ "dplaId")
          .getOrElse(throw new RuntimeException("Missing required DPLA ID when writing payload assets to S3"))

        // TODO add Payload URIs to json record to be indexed so that files which doesn't exist in s3 are not included
        // in the search index, also the search index should only contain links to assets in the DPLA s3 bucket
        val payloadUris = ebook.payload.map(payload => {
          // Write to s3 if data is not null
          if (payload.data != null) {
            // writes to s3 and returns the full path to the s3 object
            S3Writer.writePayloadToS3(payload, dplaId)
          } else
            "" // if no data then return empty string and then filter out
        }).filter(_.nonEmpty)

        // copy mapping of payload URIs to media master
        // FIXME me this is a temporary kludge to embed assets into model and therefore into the search index
        // To be removed or rewritten when Payloads are modeled and correctly mapped
        ebook.copy(oreAggregation = ebook.oreAggregation.copy(
          mediaMaster = payloadUris.map(stringOnlyWebResource))
        )
    }
  }

  /**
   * Create indexable JSON dataset
   *  - Maps CH OreAggregation model to ES schema
   *  - Cleans up ES record
   *
   * @param ebooks
   * @param spark
   * @return
   */
  def createIndexDataset(ebooks: Dataset[Ebook], spark: SparkSession): Dataset[String] = {
    import spark.implicits._

    ebooks.map(ebook => {
      Try {
        // converts metadata ebook record to ES schema
        val jsonRecord: String = jsonlRecord(ebook.oreAggregation)
        // cleans up schema for ES
        transformRecord(jsonRecord)
      } match {
        case Success(json) => Option(json)
        case Failure(f) =>
          println(s"Error generating JSON because ${f.getMessage}")
          None
      }
    }).flatMap(json => json)
  }

  /**
   *
   * @param indexData
   * @param spark
   * @param host
   * @param port
   * @param indexName
   */
  def writeToIndex(indexData: Dataset[String],
                   spark: SparkSession,
                   host: String,
                   port: String,
                   indexName: String): Unit = {

    val indexJson: RDD[String] = indexData.rdd

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

  /**
   * Copied from sparkindexer project
   *
   * @param recordString
   * @return
   */
  def transformRecord(recordString: String): String = {
    val j: JValue = JsonMethods.parse(recordString)
    val source: JValue = j \ "_source"
    // There are fields in legacy data files that we either don't need in
    // Ingestion 3, or that are forbidden by Elasticsearch 6:
    val cleanSource: JValue = source.removeField {
      case ("_id", _) => true
      case ("_rev", _) => true
      case ("ingestionSequence", _) => true
      case _ => false
    }
    JsonMethods.compact(cleanSource) // "compact" String rendering
  }
}
