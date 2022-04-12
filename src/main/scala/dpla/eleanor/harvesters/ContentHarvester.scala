package dpla.eleanor.harvesters

import java.net.URL
import java.security.MessageDigest
import java.util.concurrent.TimeUnit

import dpla.eleanor.Schemata.{HarvestData, Payload}
import okhttp3.{OkHttpClient, Request}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.{Failure, Success, Try}


/**
  * Responsible for harvesting content (e.g. files)
  */
class ContentHarvester extends Retry with Serializable {

  lazy val httpClient: OkHttpClient = new OkHttpClient.Builder()
    .connectTimeout(20, TimeUnit.SECONDS)
    .readTimeout(20, TimeUnit.SECONDS)
    .retryOnConnectionFailure(true)
    .followRedirects(true)
    .build()

  /**
    *
    * Downloads all files specified in the harvest payloads
    *
    * @param ds Dataset[HarvestData] Harvested data
    * @param spark Spark Session
    * @return HarvestDataset with complete payload objects (payload file objects)
    */
  def harvestContent(ds: Dataset[HarvestData], spark: SparkSession): Dataset[HarvestData] = {
    import spark.implicits._

    ds.map(record => {
      val payloads: Seq[Payload] = record
        .payloads
        .map(payload => download(payload))
      record.copy(payloads = payloads)
    }).cache()
  }

  def download(target: Payload): Payload = {
    val targetUrl = Try { new URL(target.url) } match {
      case Success(url) => url
      case Failure(f) =>
        println(s"Error -- no valid target for download at ${target.url}")
        return target
    }

    val path = targetUrl.getPath

    val filename = Try { path.substring(path.lastIndexOf("/")).replaceFirst("/", "") } match {
      case Success(s) => s
      case Failure(_) =>
        println(s"Unable to identify filename from $path")
        path // Unable to identify a file name from the last component of the path then use the URL
    }

    var payload: Payload = Payload()

    retry(5) {
        val request = new Request.Builder().url(targetUrl).build()
        println(s"Requesting...${targetUrl}")
        val response = httpClient.newCall(request).execute()
        if (response.isSuccessful) {
          // handle unspecified content-type header
          val mimeType = Try { response.header("content-type") } match {
            case Success(s) => s
            case Failure(_) => ""
          }
          val size = response.body().contentLength()
          // may fail if file too large
          val data = response.body().bytes()

          val checksum = MessageDigest.getInstance("SHA-512") digest data
          val sha512 = checksum.map("%02X" format _).mkString

          println(s"Downloaded $filename from $targetUrl")
          payload = target.copy(
            filename = filename,
            mimeType = mimeType,
            size = size,
            data = data,
            sha512 = sha512
          )
        }
      } match {
        // return original (incomplete) payload
        case Failure(e) =>
          println(s"Failed to download $targetUrl")
          target
        // return complete payload
        case Success(_) => payload
      }
  }
}
