package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.confs.i3Conf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.concurrent.{Callable, Executors}
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.matching.Regex
import scala.xml.Utility

/** JSTOR (Ithaka) two-step OAI harvester.
  *
  * JSTOR's OAI records carry only a `Medias:` *endpoint* URL (a JSON listing) and a
  * paywalled `FullSize:` URL — not the real public master media. This harvester runs
  * the standard OAI harvest (via [[LocalOaiHarvester]]) and then, in the same step,
  * resolves each record's `Medias:` endpoint and injects one
  * `<oai_dc:identifier>Media:<url></oai_dc:identifier>` per public media (ordered by
  * the listing's `sequence_number`) just before the closing `</oai_dc:dc>` tag.
  * [[dpla.ingestion3.mappers.providers.IthakaMapping]] reads the `Media:` prefix into
  * `mediaMaster` and excludes it from `identifier`.
  *
  * Media resolution runs as a Spark `mapPartitions` pass over the harvested records,
  * using a bounded per-partition thread pool for concurrency (the work is I/O bound).
  * It is best-effort by design: `mediaMaster` is optional and raises no mapping
  * warning, so records whose `Medias:` fetch fails (timeout/5xx) or that carry no
  * `Medias:` endpoint are written through unchanged rather than failing the harvest.
  */
class IthakaOaiHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf,
    harvestLogger: OaiHarvestLogger = OaiHarvestLogger.Noop
) extends LocalOaiHarvester(spark, shortName, conf, harvestLogger) {

  import IthakaOaiHarvester._

  override def harvest: DataFrame = {
    val base = super.harvest
    val schema = base.schema
    val docIdx = schema.fieldIndex("document")

    // Resolve the Medias endpoint for every record and inject the resulting public
    // media URLs, concurrently within each Spark partition.
    val augmented = base.rdd.mapPartitions { part =>
      val rows = part.toVector
      if (rows.isEmpty) Iterator.empty
      else {
        val client = HttpClient
          .newBuilder()
          .connectTimeout(Duration.ofSeconds(HttpTimeoutSeconds))
          // Redirects are followed manually in fetchJson so each Location target
          // can be validated against ApprovedApexDomains before the request is sent.
          .followRedirects(HttpClient.Redirect.NEVER)
          .build()
        val pool = Executors.newFixedThreadPool(Threads)
        try {
          val tasks: java.util.List[Callable[Row]] = rows.map { row =>
            (() => {
              val document = row.getString(docIdx)
              if (document == null) row
              else {
                val urls = resolveMediaUrls(document, client)
                if (urls.isEmpty) row
                else Row.fromSeq(row.toSeq.updated(docIdx, injectMedia(document, urls)))
              }
            }): Callable[Row]
          }.asJava
          pool.invokeAll(tasks).asScala.map(_.get).iterator
        } finally pool.shutdown()
      }
    }

    // Force the network pass to run here — once, inside harvest() — rather than
    // lazily at the executor's write action. The augmented RDD reads from the temp
    // Avro that cleanUp() deletes after harvest returns, and the executor may take
    // more than one action on the result; cache + count materializes it eagerly so
    // it neither re-runs nor depends on the temp Avro surviving.
    val augmentedDf = spark.createDataFrame(augmented, schema)
    augmentedDf.cache()
    augmentedDf.count()
    augmentedDf
  }
}

object IthakaOaiHarvester {

  /** Per-partition concurrency for the (I/O-bound) Medias fetches. */
  private val Threads = 16
  private val HttpTimeoutSeconds = 15L
  private val MaxRedirects = 5
  private val MediasRegex: Regex = """Medias:\s*(https?://[^\s<"']+)""".r
  // Closing tags of the metadata payload, in preference order. The injected
  // Media: identifiers go immediately before whichever appears first.
  private val DcCloseTags = Seq("</oai_dc:dc>", "</dc:dc>", "</dc>")

  // Hosts we permit for the initial Medias URL and every redirect target.
  private[oai] val ApprovedApexDomains: Set[String] =
    Set("jstor.org", "ithaka.org", "artstor.org")

  /** True iff `host` is under one of the approved apex domains. */
  private[oai] def isApprovedHost(host: String): Boolean = {
    if (host == null) return false
    val h = host.toLowerCase
    ApprovedApexDomains.exists(apex => h == apex || h.endsWith("." + apex))
  }

  /** True iff `host` is a private, loopback, link-local, or reserved address.
    * Uses string matching only — no DNS resolution.
    */
  private[oai] def isPrivateHost(host: String): Boolean = {
    if (host == null) return true
    val h = host.toLowerCase.stripSuffix(".")
    if (h == "localhost" || h.endsWith(".localhost") ||
        h == "::1"       || h == "[::1]") return true
    val IPv4 = """^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$""".r
    h match {
      case IPv4(a, b, _, _) =>
        val (ai, bi) = (a.toInt, b.toInt)
        ai == 10 ||                             // 10.0.0.0/8
        ai == 127 ||                            // 127.0.0.0/8 loopback
        (ai == 172 && bi >= 16 && bi <= 31) ||  // 172.16.0.0/12
        (ai == 192 && bi == 168) ||             // 192.168.0.0/16
        (ai == 169 && bi == 254) ||             // 169.254.0.0/16 link-local
        ai == 0 ||                              // 0.0.0.0/8
        ai >= 224                               // multicast / reserved
      case _ =>
        h.endsWith(".local") || h.endsWith(".internal") ||
        h.startsWith("[fc")  || h.startsWith("[fd") ||  // IPv6 ULA
        h.startsWith("[fe80")                           // IPv6 link-local
    }
  }

  /** The initial Medias URL must be HTTPS and on an approved host. */
  private[oai] def isValidInitialUrl(url: String): Boolean =
    Try {
      val uri = URI.create(url)
      "https".equalsIgnoreCase(uri.getScheme) &&
      isApprovedHost(uri.getHost) &&
      !isPrivateHost(uri.getHost)
    }.getOrElse(false)

  /** Redirect targets must be on an approved, non-private host.
    * HTTP is permitted because JSTOR's Medias endpoint legitimately downgrades
    * https→http on its first redirect; blocking it would drop all media resolution.
    */
  private[oai] def isValidRedirectUrl(url: String): Boolean =
    Try {
      val uri = URI.create(url)
      val s = uri.getScheme
      (s.equalsIgnoreCase("https") || s.equalsIgnoreCase("http")) &&
      isApprovedHost(uri.getHost) &&
      !isPrivateHost(uri.getHost)
    }.getOrElse(false)

  /** Decode the five standard XML entities in a URL extracted from an XML document. */
  private[oai] def decodeXmlEntities(s: String): String =
    s.replace("&amp;",  "&")
     .replace("&lt;",   "<")
     .replace("&gt;",   ">")
     .replace("&quot;", "\"")
     .replace("&apos;", "'")

  /** GET `url`, manually following up to `redirectsLeft` validated redirects.
    * Returns the 2xx response body, or None on any error / rejected host.
    */
  private def fetchJson(url: String, client: HttpClient, redirectsLeft: Int): Option[String] = {
    if (redirectsLeft < 0) return None
    val uri = Try(URI.create(url)).getOrElse(return None)
    val req = HttpRequest.newBuilder(uri)
      .timeout(Duration.ofSeconds(HttpTimeoutSeconds))
      .header("User-Agent", "DPLA-ingest")
      .GET()
      .build()
    val resp = Try(client.send(req, HttpResponse.BodyHandlers.ofString())).getOrElse(return None)
    resp.statusCode() / 100 match {
      case 2 => Some(resp.body())
      case 3 =>
        val locOpt = resp.headers().firstValue("Location")
        if (!locOpt.isPresent) None
        else {
          val loc     = locOpt.get()
          val absLoc  = Try(uri.resolve(loc).toString).getOrElse(return None)
          if (!isValidRedirectUrl(absLoc)) None
          else fetchJson(absLoc, client, redirectsLeft - 1)
        }
      case _ => None
    }
  }

  /** GET the Medias endpoint referenced in the document and return its public media
    * URLs ordered by `sequence_number`. Returns empty on no endpoint, invalid URL,
    * unapproved host, or any fetch failure.
    */
  private[oai] def resolveMediaUrls(document: String, client: HttpClient): Seq[String] =
    MediasRegex.findFirstMatchIn(document).map(_.group(1)) match {
      case None => Seq.empty
      case Some(rawUrl) =>
        val url = decodeXmlEntities(rawUrl)
        if (!isValidInitialUrl(url)) Seq.empty
        else Try(fetchJson(url, client, MaxRedirects)).toOption.flatten
          .map(parseMediaUrls).getOrElse(Seq.empty)
    }

  /** Parse a Medias JSON listing (an array of media objects) into media URLs ordered
    * by `sequence_number`. Tolerant of malformed input (returns empty).
    */
  private[oai] def parseMediaUrls(body: String): Seq[String] =
    Try {
      parse(body) match {
        case JArray(items) =>
          items
            .map { m =>
              val seq = m \ "sequence_number" match {
                case JInt(n)    => n.toInt
                case JLong(n)   => n.toInt
                case JDouble(n) => n.toInt
                case _          => 0
              }
              val mediaUrl = m \ "media_url" match {
                case JString(s) => s
                case _          => ""
              }
              (seq, mediaUrl)
            }
            .filter(_._2.nonEmpty)
            .sortBy(_._1)
            .map(_._2)
        case _ => Seq.empty
      }
    }.getOrElse(Seq.empty)

  /** Inject one `<oai_dc:identifier>Media:<url></oai_dc:identifier>` per url just
    * before the metadata payload's closing tag. Leaves the document unchanged if no
    * closing tag is found.
    */
  private[oai] def injectMedia(document: String, urls: Seq[String]): String =
    if (urls.isEmpty) document
    else {
      val blob = urls
        .map(u => s"<oai_dc:identifier>Media:${Utility.escape(u)}</oai_dc:identifier>")
        .mkString
      DcCloseTags.iterator.map(document.indexOf).find(_ >= 0) match {
        case Some(i) => document.substring(0, i) + blob + document.substring(i)
        case None    => document
      }
    }
}
