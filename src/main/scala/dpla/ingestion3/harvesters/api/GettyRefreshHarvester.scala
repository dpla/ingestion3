package dpla.ingestion3.harvesters.api

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.SeedsFromPreviousHarvest
import dpla.ingestion3.model.AVRO_MIME_JSON
import dpla.ingestion3.utils.Utils
import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JArray, JString, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.net.{URI, URL, URLEncoder}
import java.nio.charset.StandardCharsets
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Duration, LocalDate}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, Executors, TimeUnit}
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try, Using}

/** Harvests Getty by looking up known record ids one at a time.
  *
  * WHY THIS EXISTS
  * ---------------
  * Getty's Primo gateway caps any single query at `offset <= 1999` and
  * `limit <= 1000`, so the deepest record reachable by one query is ~2,999 of
  * ~101,400. [[GettyHarvester]] pages by offset and therefore returns ~2,000
  * records and reports SUCCESS -- a silent 98% shortfall that reached
  * production in February 2026.
  *
  * A single record IS retrievable by id, and no offset applies to a one-record
  * result:
  *
  * {{{
  *   q=rid,exact,GETTY_ROSETTAIE10176553  ->  exactly 1 record
  * }}}
  *
  * So this harvests by looking up every id we already know. Coverage is exact
  * by construction: one lookup per id, and ids that return no document are
  * recorded as gone (removal candidates for the index).
  *
  * DISCOVERY
  * ---------
  * A seeded lookup cannot, by itself, find an id we do not already hold. Getty's
  * view exposes a `newrecords` facet -- cumulative "07/30/90 days back" windows --
  * so a second pass asks for everything added in the last 90 days and merges
  * anything new. That pass is a handful of records and a couple of requests.
  *
  * WHAT THIS STILL DOES NOT GUARANTEE
  * ----------------------------------
  * This is a workaround for the offset cap, not a complete harvest:
  *
  *   - `newrecords` tops out at 90 days. A quarterly schedule has essentially no
  *     margin: records added more than 90 days before a run are invisible to the
  *     discovery pass and, if never seeded, are missed permanently.
  *   - The GETTY_OCP side of the view -- 78,613 of 101,393 records -- carries
  *     almost no facets at all (rtype and local2 each collapse to a single value
  *     covering every record; genre covers a few hundred). It cannot be
  *     partitioned, so it cannot be enumerated. OCP coverage rests entirely on
  *     the seed. That has held because OCP has not gained a record since 2021 --
  *     its maximum id is unchanged -- but if Getty ever adds to OCP, only the
  *     90-day window would catch it.
  *
  * So: good for keeping the aggregation from drifting, not a guarantee that DPLA
  * holds every record Getty publishes. The real fix is Ex Libris lifting the
  * offset cap (at which point [[GettyHarvester]] becomes correct again) or Getty
  * providing a bulk feed.
  *
  * THE SEED
  * --------
  * The ids come from the hub's own previous harvest, found automatically: the
  * `harvest` activity directory is listed and the newest completed run wins.
  * `OutputHelper` names those directories `YYYYMMDD_HHMMSS-<hub>-<schema>`, so
  * the convention is stable and the timestamp sorts lexically. Nothing needs
  * configuring, and carry-forward is automatic -- each run's output is the next
  * run's seed, so ids discovered in one run are never lost in the next.
  *
  * `getty.harvest.seed` overrides that when the automatic answer is wrong:
  * re-seeding from a specific older harvest, or from a hand-built id file during
  * a backfill. It is not meant to be edited between routine ingests.
  *
  * @param spark
  *   Spark session
  * @param shortName
  *   Provider short name
  * @param conf
  *   Configuration
  */
class GettyRefreshHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends ApiHarvester(shortName, conf)
    with SeedsFromPreviousHarvest {

  import GettyRefreshHarvester._

  private val logger = LogManager.getLogger(this.getClass)

  private val httpClient: HttpClient = HttpClient
    .newBuilder()
    .connectTimeout(Duration.ofMillis(TimeoutMillis.toLong))
    .followRedirects(HttpClient.Redirect.NORMAL)
    .build()

  def mimeType: GenericData.EnumSymbol = AVRO_MIME_JSON

  override protected val queryParams: Map[String, String] =
    Map("api_key" -> conf.harvest.apiKey)
      .collect { case (key, Some(value)) => key -> value }

  private val apiKey: String = conf.harvest.apiKey.getOrElse(
    throw new RuntimeException(
      "getty.harvest.apiKey is not set -- the key is read from i3.conf, never hardcoded"
    )
  )

  /** Where the ids come from.
    *
    * Normally nothing is configured: the previous harvest is found by listing
    * this hub's `harvest` activity directory and taking the newest complete one.
    * `OutputHelper` names those directories `YYYYMMDD_HHMMSS-<hub>-<schema>`, so
    * they sort by name and the convention is stable.
    *
    * `getty.harvest.seed` overrides that, for the cases where the automatic
    * answer is the wrong one -- re-seeding from a specific older harvest, or
    * from a hand-built id file during a backfill. It is not meant to be edited
    * between routine ingests.
    */
  private lazy val seedPath: String = conf.harvest.seed
    .map { configured =>
      logger.info(s"Seed overridden by getty.harvest.seed: $configured")
      configured
    }
    .orElse(previousHarvestIn(dataRoot))
    .getOrElse(
      throw new RuntimeException(
        s"No previous harvest found to seed from. Looked under " +
          s"${dataRoot.map(harvestDirFor).getOrElse("<no --output supplied>")}. " +
          s"The first run against a hub with no harvest history needs " +
          s"getty.harvest.seed pointed at one (an OriginalRecord.avro directory " +
          s"or a newline-delimited id file)."
      )
    )

  /** The hub's harvest activity directory under a data root. */
  private def harvestDirFor(root: String): String =
    if (root.endsWith("/")) s"$root$shortName/harvest" else s"$root/$shortName/harvest"

  /** Newest complete harvest under `root`, if there is one.
    *
    * Uses the Hadoop FileSystem API so a local path and an `s3a://` bucket are
    * handled identically -- `--output` can be either.
    */
  private def previousHarvestIn(root: Option[String]): Option[String] = root.flatMap { r =>
    val dir = harvestDirFor(r)
    Try {
      val path = new HPath(dir)
      val fs = path.getFileSystem(spark.sparkContext.hadoopConfiguration)
      if (!fs.exists(path)) Seq.empty
      else
        fs.listStatus(path)
          .filter(_.isDirectory)
          .map(_.getPath.getName)
          .toSeq
          // Skip incomplete Spark writes: a directory with _temporary and no
          // _SUCCESS is a crashed run, and seeding from one would silently
          // shrink the harvest.
          .filter(name => fs.exists(new HPath(s"$dir/$name/_SUCCESS")))
    } match {
      case Success(names) =>
        val chosen = newestHarvest(names, shortName)
        chosen match {
          case Some(name) => logger.info(s"Seeding from previous harvest: $dir/$name")
          case None       => logger.warn(s"No completed harvest found under $dir")
        }
        chosen.map(name => s"$dir/$name")
      case Failure(e) =>
        logger.warn(s"Could not list $dir (${Option(e.getMessage).getOrElse(e.toString)})")
        None
    }
  }

  /** Seconds each worker waits between its own requests. */
  private val restSeconds: Double =
    conf.harvest.sleep.flatMap(s => Try(s.toDouble).toOption).getOrElse(DefaultRestSeconds)

  override def harvest: DataFrame = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    val ids = loadSeedIds(seedPath)
    if (ids.isEmpty)
      throw new RuntimeException(s"Seed at $seedPath yielded no record ids; refusing to harvest nothing.")
    logger.info(s"Seed: ${Utils.formatNumber(ids.size.toLong)} distinct record ids from $seedPath")

    // A 403 on the very first lookup means the allowlisted route is not applied.
    // Fail immediately rather than working through the whole seed against a
    // blocked endpoint -- Getty's gateway is a partner's production system.
    probeRoute(ids.head)

    val found = new AtomicInteger(0)
    val gone = new AtomicInteger(0)
    val failed = new AtomicInteger(0)
    val consecutiveForbidden = new AtomicInteger(0)
    val lastLogged = new AtomicLong(0)
    val deadIds = new ConcurrentLinkedQueue[String]()
    @volatile var routeLost: Option[String] = None

    // The Avro writer inherited from LocalHarvester is NOT thread-safe, so every
    // write goes through this monitor. Lookups are what take the time; writes are
    // microseconds, so serialising them costs nothing.
    val writeLock = new Object

    val work = new ConcurrentLinkedQueue[String]()
    ids.foreach(work.add)

    val pool = Executors.newFixedThreadPool(Workers)
    val latch = new CountDownLatch(Workers)
    val total = ids.size

    (1 to Workers).foreach { _ =>
      pool.submit(new Runnable {
        override def run(): Unit =
          try {
            var id = work.poll()
            while (id != null && routeLost.isEmpty) {
              lookup(id) match {
                case Retrieved(doc) =>
                  consecutiveForbidden.set(0)
                  writeLock.synchronized {
                    saveOutRecords(List(ApiRecord(id, compact(render(doc)))))
                  }
                  found.incrementAndGet()
                case Gone =>
                  consecutiveForbidden.set(0)
                  deadIds.add(id)
                  gone.incrementAndGet()
                case Forbidden(message) =>
                  failed.incrementAndGet()
                  if (consecutiveForbidden.incrementAndGet() >= RouteLostAfter)
                    routeLost = Some(message)
                case LookupFailed(message) =>
                  consecutiveForbidden.set(0)
                  failed.incrementAndGet()
                  logger.warn(s"Lookup failed for $id: $message")
              }

              val done = found.get() + gone.get() + failed.get()
              if (done - lastLogged.get() >= LogEvery) {
                lastLogged.set(done.toLong)
                logger.info(
                  s"Looked up ${Utils.formatNumber(done.toLong)} of ${Utils.formatNumber(total.toLong)} " +
                    s"(${Utils.formatNumber(found.get().toLong)} retrieved, " +
                    s"${Utils.formatNumber(gone.get().toLong)} gone, ${failed.get()} failed)"
                )
              }
              id = work.poll()
            }
          } finally latch.countDown()
      })
    }

    pool.shutdown()
    latch.await()
    pool.awaitTermination(1, TimeUnit.MINUTES)

    routeLost.foreach { message =>
      close()
      throw new RuntimeException(
        s"Aborted after $RouteLostAfter consecutive 403s -- the allowlisted route is gone " +
          s"($message). Stopped rather than continuing to call the partner's endpoint. " +
          s"Held ${Utils.formatNumber(found.get().toLong)} records."
      )
    }

    logger.info(
      s"Refresh complete: ${Utils.formatNumber(found.get().toLong)} retrieved, " +
        s"${Utils.formatNumber(gone.get().toLong)} gone, ${failed.get()} failed, " +
        s"of ${Utils.formatNumber(total.toLong)} seeded"
    )

    discoveryGapWarning(seedPath, LocalDate.now()).foreach { warning =>
      logger.warn("!" * 78)
      logger.warn(s"GETTY DISCOVERY GAP: $warning")
      logger.warn("!" * 78)
    }

    val discovered = harvestNewRecords(ids.toSet, writeLock)
    if (discovered > 0)
      logger.info(s"Discovery: $discovered record(s) new since the last harvest")
    if (!deadIds.isEmpty) {
      val sample = deadIds.toArray.take(20).mkString(", ")
      logger.info(
        s"${deadIds.size} ids no longer resolve and are removal candidates. First: $sample"
      )
    }
    // A seed that mostly fails to resolve means something is wrong upstream, not
    // that Getty withdrew 90% of its collection. Refuse to publish that.
    val resolved = found.get().toDouble / total
    if (resolved < MinResolvedFraction)
      throw new RuntimeException(
        f"Only ${resolved * 100}%.1f%% of seeded ids resolved (${found.get()} of $total). " +
          f"Expected at least ${MinResolvedFraction * 100}%.0f%%. Refusing to publish a " +
          f"harvest this far below the seed; investigate before re-running."
      )

    // Flush and close the Avro writer before Spark reads the file (issue #760).
    close()
    spark.read.format("avro").load(tmpOutStr)
  }

  /** One lookup, with retries. Does not retry a 403 -- that is the route, not luck. */
  private def lookup(recordId: String): LookupOutcome = {
    var attempt = 0
    var last: LookupOutcome = LookupFailed("no attempt made")
    while (attempt < MaxRetries) {
      if (attempt > 0) Thread.sleep((RetryBackoffSeconds * attempt * 1000).toLong)
      last = singleLookup(recordId)
      last match {
        case _: Retrieved    => return last
        case Gone            => return last
        case _: Forbidden    => return last
        case _: LookupFailed => attempt += 1
      }
    }
    last
  }

  private def singleLookup(recordId: String): LookupOutcome = {
    val url = lookupUrl(apiKey, recordId)
    val outcome = Try(readBody(url)) match {
      case Success(body) =>
        Try(parse(body)) match {
          case Success(json) =>
            docsOf(json).headOption.map(Retrieved).getOrElse(Gone)
          case Failure(e) => LookupFailed(s"unparseable response: ${e.getMessage}")
        }
      case Failure(e) =>
        val message = Option(e.getMessage).getOrElse(e.toString)
        if (isForbidden(message)) Forbidden(message) else LookupFailed(message)
    }
    // Pace AFTER the body is read and the connection is closed. Sleeping with the
    // connection open and the body unread makes the server drop it, which surfaces
    // as connection resets that look like rate limiting and get "fixed" by
    // increasing the delay -- which makes it strictly worse.
    Thread.sleep((restSeconds * 1000).toLong)
    outcome
  }

  /** Fail fast when the first lookup is refused: the exit node is not applied. */
  private def probeRoute(sampleId: String): Unit = singleLookup(sampleId) match {
    case Forbidden(message) =>
      throw new RuntimeException(
        s"First lookup was refused ($message). Getty only accepts the allowlisted " +
          s"egress -- route through the Tailscale exit node before harvesting. " +
          s"Verify with: curl -s https://checkip.amazonaws.com"
      )
    case _ => ()
  }

  /** Fetches records Getty flagged as added within the widest `newrecords`
    * window, writing any whose id is not already in the seed.
    *
    * Getty's windows are cumulative (07 days back is a subset of 30, which is a
    * subset of 90), so only the widest is worth asking for. Counts here are tens
    * of records, far inside the offset cap, but page anyway rather than assume.
    *
    * @return the number of previously unknown records written
    */
  private def harvestNewRecords(seeded: Set[String], writeLock: AnyRef): Int = {
    var written = 0
    val seenHere = mutable.Set.empty[String]
    var offset = 0
    var exhausted = false

    while (!exhausted && offset <= MaxOffset) {
      val url = newRecordsUrl(apiKey, WidestNewRecordsWindow, offset, PageLimit)
      Try(parse(readBody(url))) match {
        case Success(json) =>
          val docs = docsOf(json)
          docs.foreach { doc =>
            recordIdOf(doc).foreach { id =>
              if (!seeded.contains(id) && seenHere.add(id)) {
                writeLock.synchronized {
                  saveOutRecords(List(ApiRecord(id, compact(render(doc)))))
                }
                written += 1
                logger.info(s"Discovered new record $id")
              }
            }
          }
          if (docs.size < PageLimit) exhausted = true else offset += PageLimit
        case Failure(e) =>
          // Discovery is additive. Losing it costs us new records, not the
          // refresh we already completed, so warn loudly and return.
          logger.warn(
            s"Discovery pass failed (${Option(e.getMessage).getOrElse(e.toString)}). " +
              s"The refresh stands, but records added in the last 90 days may be missing."
          )
          exhausted = true
      }
      Thread.sleep((restSeconds * 1000).toLong)
    }
    written
  }

  /** One GET, no hidden retries.
    *
    * [[dpla.ingestion3.utils.HttpUtils.makeGetRequest]] retries internally, which
    * would multiply every 403 against a partner endpoint that has already refused
    * us and would make the circuit breaker below count wrong. Retries belong to
    * [[lookup]], which knows a 403 is not worth retrying.
    */
  private def readBody(url: URL): String = {
    val request = HttpRequest
      .newBuilder(URI.create(url.toString))
      .timeout(Duration.ofMillis(TimeoutMillis.toLong))
      .header("Accept", "application/json")
      .GET()
      .build()
    val response = httpClient.send(request, HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() == 200) response.body()
    else
      throw new RuntimeException(
        s"Code: ${response.statusCode()} Message: ${response.body().take(200)}"
      )
  }

  /** Reads the previous harvest's ids, from an Avro directory or a text file. */
  private def loadSeedIds(path: String): Seq[String] =
    if (path.endsWith(".txt") || path.endsWith(".ids")) {
      Using.resource(Source.fromFile(path))(src => parseIdFile(src.getLines()))
    } else {
      spark.read
        .format("avro")
        .load(path)
        .select("id")
        .distinct()
        .collect()
        .flatMap(row => Option(row.getString(0)))
        .filter(_.nonEmpty)
        .toSeq
    }
}

object GettyRefreshHarvester {

  val Endpoint = "https://api-na.hosted.exlibrisgroup.com/primo/v1/search"

  /** The DPLA facet -- what Getty publishes to DPLA. `local5` is a Primo local
    * field Getty populates through its own publishing pipes; Getty has stated
    * they hold no documentation of the selection criteria.
    */
  val DplaFacetQuery = "facet_local5,exact,DPLA"

  /** The DPLA view. `loc` is unnecessary: per Getty the view already contains
    * only GETTY_OCP and GETTY_ROSETTA.
    */
  val ViewParams: Seq[(String, String)] = Seq(
    "vid" -> "DPLA",
    "tab" -> "dpla",
    "scope" -> "DPLA",
    "inst" -> "01GRI",
    "lang" -> "eng"
  )

  /** Getty's `newrecords` windows are cumulative, so only the widest is useful. */
  val WidestNewRecordsWindow = "90 days back"

  /** Days covered by [[WidestNewRecordsWindow]]. Records added before this many
    * days ago are invisible to the discovery pass -- if they were never seeded,
    * nothing will ever find them. Hence [[discoveryGapWarning]].
    */
  val DiscoveryWindowDays = 90L

  /** Activity directories are named `YYYYMMDD_HHMMSS-<hub>-<schema>`. */
  private val ActivityTimestamp = """(\d{8})_\d{6}-""".r

  /** Picks the newest harvest activity directory from a listing.
    *
    * [[dpla.ingestion3.dataStorage.OutputHelper]] names them
    * `YYYYMMDD_HHMMSS-<hub>-OriginalRecord.avro`. That timestamp is fixed-width
    * and zero-padded, so lexical order is chronological order and no parsing is
    * needed to rank them. Names that do not match are ignored rather than
    * guessed at -- anything else in the directory is not a harvest.
    */
  def newestHarvest(names: Seq[String], shortName: String): Option[String] = {
    val pattern = ("""^\d{8}_\d{6}-""" + java.util.regex.Pattern.quote(shortName) +
      """-OriginalRecord\.avro$""").r
    Option(names).getOrElse(Seq.empty).filter(n => pattern.pattern.matcher(n).matches()) match {
      case Nil     => None
      case matches => Some(matches.max)
    }
  }

  /** The date encoded in a harvest activity path, if it has one. */
  def previousHarvestDate(seedPath: String): Option[LocalDate] =
    ActivityTimestamp
      .findAllMatchIn(Option(seedPath).getOrElse(""))
      .map(_.group(1))
      .toSeq
      .lastOption
      .flatMap(d => Try(LocalDate.parse(d, DateTimeFormatter.BASIC_ISO_DATE)).toOption)

  /** Warns when more time has passed since the previous harvest than the
    * discovery window covers.
    *
    * Beyond 90 days there is a blind spot: records Getty added after the last
    * harvest but before the window opens are neither in the seed nor returned by
    * `newrecords`, and no later run will find them either. The operator needs to
    * know the run cannot be trusted for completeness -- hence a warning rather
    * than a silent gap. It is not fatal: a late harvest is still far better than
    * none, and refusing to run would make the problem worse.
    */
  def discoveryGapWarning(seedPath: String, now: LocalDate): Option[String] =
    previousHarvestDate(seedPath).flatMap { previous =>
      val days = ChronoUnit.DAYS.between(previous, now)
      if (days <= DiscoveryWindowDays) None
      else
        Some(
          s"$days days have passed since the previous harvest ($previous), but Getty's " +
            s"`newrecords` facet only reaches back $DiscoveryWindowDays days. Records added " +
            s"between $previous and ${now.minusDays(DiscoveryWindowDays)} are in neither the " +
            s"seed nor the discovery window, and no later run will find them. This harvest " +
            s"cannot be treated as complete. Shorten the interval between Getty harvests."
        )
    }

  /** The gateway caps offset at 1999 and limit at 1000. */
  val MaxOffset = 1999
  val PageLimit = 1000

  val Workers = 6
  val DefaultRestSeconds = 0.5
  val MaxRetries = 4
  val RetryBackoffSeconds = 15.0
  val TimeoutMillis = 90000
  val LogEvery = 5000

  /** Consecutive 403s that mean the route is gone rather than one bad query. */
  val RouteLostAfter = 3

  /** Below this share of the seed resolving, refuse to publish the harvest. */
  val MinResolvedFraction = 0.5

  sealed trait LookupOutcome
  case class Retrieved(doc: JValue) extends LookupOutcome
  case object Gone extends LookupOutcome
  case class Forbidden(message: String) extends LookupOutcome
  case class LookupFailed(message: String) extends LookupOutcome

  /** Single-record lookup URL. `rid,exact,<id>` returns exactly one record, so
    * no offset applies and the gateway's paging cap is irrelevant.
    */
  def lookupUrl(apiKey: String, recordId: String): URL = {
    val params = ViewParams ++ Seq(
      "q" -> s"rid,exact,$recordId",
      "offset" -> "0",
      "limit" -> "1",
      "apikey" -> apiKey
    )
    val query = params
      .map { case (k, v) => s"${encode(k)}=${encode(v)}" }
      .mkString("&")
    new URL(s"$Endpoint?$query")
  }

  /** Records Getty flagged as added within `window` (a `newrecords` facet value). */
  def newRecordsUrl(apiKey: String, window: String, offset: Int, limit: Int): URL = {
    val params = ViewParams ++ Seq(
      "q" -> DplaFacetQuery,
      "multiFacets" -> s"facet_newrecords,include,$window",
      "offset" -> offset.toString,
      "limit" -> limit.toString,
      "apikey" -> apiKey
    )
    new URL(s"$Endpoint?" + params.map { case (k, v) => s"${encode(k)}=${encode(v)}" }.mkString("&"))
  }

  private def encode(s: String): String =
    URLEncoder.encode(s, StandardCharsets.UTF_8.name())

  /** The `docs` array of a Primo search response. */
  def docsOf(json: JValue): List[JValue] = json \ "docs" match {
    case JArray(docs) => docs
    case _            => Nil
  }

  /** Extracts a record id as a plain string.
    *
    * Primo nests `recordid` under `pnx.control` and wraps it in an array.
    * [[PrimoVEHarvester]] uses `(doc \\ "control" \ "recordid").toString`, which
    * yields the json4s AST's own toString (`JArray(List(JString(...)))`) rather
    * than the id -- harmless while ids are only written out, but wrong the moment
    * a harvest's ids are read back as a seed. Hence this.
    */
  def recordIdOf(doc: JValue): Option[String] =
    (doc \\ "control" \ "recordid") match {
      case JString(id)                 => Some(id).filter(_.nonEmpty)
      case JArray(JString(id) :: _)    => Some(id).filter(_.nonEmpty)
      case _                           => None
    }

  /** True when an error looks like the allowlisted route being gone. */
  def isForbidden(message: String): Boolean = {
    val m = Option(message).getOrElse("").toLowerCase
    // The gateway also returns 403 for offset overruns, which is a property of
    // the query rather than of our egress. Single-record lookups never set an
    // offset past 0, so any 403 here really is the route.
    m.contains("403") || m.contains("forbidden") || m.contains("not allowed")
  }

  /** Parses a newline-delimited id file, ignoring blanks and `#` comments. */
  def parseIdFile(lines: Iterator[String]): Seq[String] = {
    val seen = mutable.LinkedHashSet.empty[String]
    lines.foreach { raw =>
      val line = raw.trim
      if (line.nonEmpty && !line.startsWith("#")) seen += line
    }
    seen.toSeq
  }
}
