package dpla.ingestion3.harvesters.api

import dpla.ingestion3.confs.{Discovery, i3Conf}
import dpla.ingestion3.utils.HttpUtils
import org.apache.logging.log4j.LogManager
import org.json4s.JsonAST.{JArray, JString, JValue}
import org.json4s.jackson.JsonMethods.parse

import java.net.http.HttpRequest
import java.net.{URI, URL, URLEncoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.time.{Duration, LocalDate, LocalDateTime}
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Finds record ids a hub has added recently, and writes them to an id file.
  *
  * WHY THIS EXISTS SEPARATELY FROM THE HARVEST
  * -------------------------------------------
  * Primo VE caps how deep a single query can page, so DPLA harvests these hubs
  * by looking up every id it already knows (the id-seeded refresh harvester). That
  * cannot find an id we do not already hold, so a second pass asks the
  * `newrecords` facet for everything added inside its window and merges the
  * result.
  *
  * The window is the problem. It reaches back a fixed number of days -- 90 at
  * most -- so any record added more than that before a harvest is invisible to
  * the harvest, and no later run will find it either. Tying discovery to the
  * harvest therefore forces the harvest schedule to be shorter than the window,
  * and a single missed harvest loses records permanently.
  *
  * Discovery is cheap (a handful of requests) and the harvest is not, so they do
  * not belong on the same schedule. Running this weekly keeps every hub far
  * inside its window regardless of when the next harvest happens.
  *
  * WHAT IT DOES AND DOES NOT DO
  * ----------------------------
  * It writes **ids only**. It does not harvest records: a discovered id is
  * published when the next full harvest looks it up. So running this weekly does
  * not make records appear weekly -- it stops their ids from being lost.
  *
  * It is purely additive. It cannot detect deletions, and it cannot reconcile
  * its id count against the hub's total, because that total is the *net* of
  * additions and removals: a hub that adds 500 and withdraws 500 looks
  * unchanged. Corpus size is logged as a tripwire, not as a correctness check.
  *
  * STAYING UNDER THE PAGING CEILING
  * --------------------------------
  * A window can hold more records than the tenant will let us page to. Rather
  * than give up, the window is partitioned by a leading-character prefix --
  * verified to compose with the `newrecords` facet -- and each slice paged
  * separately, recursing while a slice is still too large. Only a slice that
  * cannot be split any further and still exceeds the ceiling is fatal.
  */
class PrimoIdDiscovery(shortName: String, conf: i3Conf, dataRoot: String) {

  import PrimoIdDiscovery._

  private val logger = LogManager.getLogger(this.getClass)

  private val d: Discovery = conf.harvest.discovery

  private val endpoint: String = d.endpoint.getOrElse(DefaultEndpoint)

  private val apiKey: String = d.apiKey
    .orElse(conf.harvest.apiKey)
    .getOrElse(
      throw new RuntimeException(
        s"$shortName: no API key. Set harvest.discovery.apiKey or harvest.apiKey."
      )
    )

  private val viewParams: Seq[(String, String)] =
    parseViewParams(d.viewParams.getOrElse(""))

  private val baseQuery: String = d.query
    .orElse(conf.harvest.query)
    .getOrElse(
      throw new RuntimeException(
        s"$shortName: no base query. Set harvest.discovery.query or harvest.query."
      )
    )

  private val facetName: String = d.newRecordsFacet.getOrElse(DefaultFacet)

  private val windows: Seq[String] =
    splitList(d.windows.getOrElse(DefaultWindows))

  // Required, not defaulted. These are the one thing here that is a property of
  // the *tenant* rather than of Primo: Getty allows offset+limit <= 2999,
  // Mississippi's guest-tier key allows 500. A hub that inherited a default would
  // page straight past its own ceiling, so make the config say it.
  private def requiredInt(value: Option[String], key: String): Int = value
    .map(v =>
      Try(v.trim.toInt).getOrElse(
        throw new RuntimeException(s"$shortName: harvest.discovery.$key is not a number ('$v')")
      )
    )
    .getOrElse(
      throw new RuntimeException(
        s"$shortName: harvest.discovery.$key is required when discovery is enabled. " +
          s"It is the tenant's paging cap and cannot be guessed -- measure it before setting it."
      )
    )

  private val maxOffset: Int = requiredInt(d.maxOffset, "maxOffset")
  private val pageLimit: Int = requiredInt(d.pageLimit, "pageLimit")

  /** Deepest record any single query can reach on this tenant. */
  private val ceiling: Int = maxOffset + pageLimit

  private val partitionField: String =
    d.partitionField.getOrElse(DefaultPartitionField)

  private val alphabet: Seq[String] =
    splitChars(d.partitionAlphabet.getOrElse(DefaultAlphabet))

  private val partitionBaseFacets: Seq[String] =
    splitList(d.partitionBaseFacets.getOrElse(""))

  private val restMillis: Long =
    (d.restSeconds.map(_.toDouble).getOrElse(DefaultRestSeconds) * 1000).toLong

  private val maxDepth: Int = d.maxPartitionDepth.map(_.toInt).getOrElse(DefaultMaxDepth)

  private val discoveryDir: Path =
    Paths.get(dataRoot, shortName, DiscoveryDirName)

  /** Runs discovery and returns a summary. Throws only when the run cannot be
    * trusted at all; a partial result is written before any completeness failure
    * is raised, so nothing found is thrown away.
    */
  def run(now: LocalDateTime = LocalDateTime.now()): DiscoveryResult = {
    val window = widestWindow(windows).getOrElse(
      throw new RuntimeException(s"$shortName: no discovery windows configured")
    )

    // Tripwire only -- see the class comment on why this cannot verify anything.
    val corpus = countOnly(baseQuery, None)
    corpus.foreach(t => logger.info(s"$shortName: view reports $t record(s) in total"))

    val history = idFileNames()
    val known = loadKnownIds(history)
    logger.info(
      s"$shortName: ${known.size} id(s) already discovered; asking $facetName for '$window'"
    )

    gapWarning(newestIdFile(history).flatMap(idFileDate), now.toLocalDate, windowDays(window))
      .foreach { warning =>
        logger.warn("!" * 78)
        logger.warn(s"${shortName.toUpperCase} DISCOVERY GAP: $warning")
        logger.warn("!" * 78)
      }

    val root = probeFor(window, "").getOrElse(
      throw new RuntimeException(
        s"$shortName: could not read a record count for window '$window'. " +
          s"Refusing to treat an unreadable window as empty."
      )
    )
    logger.info(s"$shortName: '$window' holds ${root.total} record(s), ceiling is $ceiling")
    if (root.total > ceiling)
      logger.info(
        s"$shortName: ${root.total} exceeds the $ceiling ceiling; partitioning on $partitionField"
      )

    val found = mutable.LinkedHashSet.empty[String]
    val shortfalls = mutable.ArrayBuffer.empty[String]
    collect(window, prefix = "", probed = root, depth = 0, found, shortfalls)

    val fresh = found.diff(known)
    writeIds(fresh.toSeq, now)

    val result = DiscoveryResult(
      shortName = shortName,
      window = window,
      windowTotal = root.total,
      retrieved = found.size,
      fresh = fresh.size,
      corpusTotal = corpus,
      shortfalls = shortfalls.toSeq
    )

    logger.info(result.summary)
    result
  }

  /** Recursively slices `window` until every slice fits under the ceiling.
    *
    * A slice still too large at [[maxDepth]] is recorded as a shortfall rather
    * than silently truncated. Note that a branch making no progress -- one child
    * carrying the parent's whole count -- is not detected early: it keeps
    * splitting until the depth limit. That is deliberate, because titles that
    * differ only in a trailing number genuinely do not separate until a deep
    * prefix, and bailing at the first flat level would trade coverage for
    * requests.
    */
  private def collect(
      window: String,
      prefix: String,
      probed: Probe,
      depth: Int,
      found: mutable.LinkedHashSet[String],
      shortfalls: mutable.ArrayBuffer[String]
  ): Unit = {
    val what = if (prefix.isEmpty) s"window '$window'" else s"prefix '$prefix'"

    if (probed.total <= ceiling) {
      val slice = pageAll(window, prefix, probed)
      found ++= slice.ids
      if (slice.rows < probed.total)
        shortfalls += s"$what read ${slice.rows} of ${probed.total} row(s)"
      return
    }

    if (depth >= maxDepth) {
      shortfalls += s"$what holds ${probed.total}, above the $ceiling ceiling, " +
        s"and cannot be split further (depth $maxDepth)"
      return
    }

    var childSum = 0
    val chars = alphabet.iterator
    // Stop as soon as the children account for the parent. Every remaining
    // character can then only return 0, and each costs a request and a pause --
    // at 36 characters per level that is the bulk of a partitioned run.
    while (chars.hasNext && childSum < probed.total) {
      val child = prefix + chars.next()
      probeFor(window, child) match {
        case Some(p) if p.total == 0 => ()
        case Some(p) =>
          childSum += p.total
          collect(window, child, p, depth + 1, found, shortfalls)
        case None =>
          shortfalls += s"could not read a count for prefix '$child'"
      }
    }

    // Records whose value under `partitionField` does not start with any
    // character in the alphabet are unreachable by this decomposition. They stay
    // in the window for its full span, so a later run can still find them if the
    // alphabet is widened -- but say so rather than report a clean run.
    val residual = probed.total - childSum
    if (residual > 0)
      shortfalls += s"$what: $residual record(s) matched no configured first " +
        s"character (children summed to $childSum of ${probed.total})"
  }

  /** Pages one slice to exhaustion, continuing from the page the probe already
    * fetched.
    *
    * Returns distinct ids and the number of result rows seen. The two differ:
    * Primo groups related holdings, so one record can occupy several rows and
    * `info.total` counts rows, not distinct records. Completeness is therefore
    * measured in rows -- a slice where every row was read is complete even
    * though it yielded fewer ids than `info.total`.
    */
  private def pageAll(window: String, prefix: String, probed: Probe): PagedSlice = {
    val ids = mutable.LinkedHashSet.empty[String]
    var rows = 0

    // A full page means there may be more behind it; a short one ends the slice.
    def take(docs: Seq[JValue]): Boolean = {
      rows += docs.size
      idsFrom(docs).foreach(ids.add)
      docs.size == pageLimit
    }

    var more = take(probed.docs)

    // The offsets are fixed by the tenant's ceiling, so walk a precomputed
    // sequence rather than stepping a cursor. A clamped step can equal its own
    // predecessor, and a cursor that stops advancing while the page stays full
    // re-issues one request at the partner forever.
    val remaining = pageOffsets(pageLimit, maxOffset).drop(1).iterator
    while (more && remaining.hasNext) {
      val offset = remaining.next()
      val url = searchUrl(qFor(prefix), Some(facetsFor(window, prefix)), offset, pageLimit)
      Try(parse(readBody(url))) match {
        case Success(json) => more = take(docsOf(json))
        case Failure(e) =>
          throw new RuntimeException(
            s"$shortName: paging failed at offset $offset for q='${qFor(prefix)}' " +
              s"(${messageOf(e)}). Stopping rather than writing a partial slice " +
              s"that would look complete.",
            e
          )
      }
      Thread.sleep(restMillis)
    }

    if (rows < probed.total)
      logger.warn(s"$shortName: q='${qFor(prefix)}' read $rows of ${probed.total} row(s)")
    PagedSlice(ids.toSeq, rows)
  }

  /** Counts a slice and fetches its first page in one request.
    *
    * Asking for the count with `limit=1` and then re-issuing the same query for
    * page one doubles the request count of every slice, and a slice smaller than
    * one page is the common case when partitioning.
    */
  private def probeFor(window: String, prefix: String): Option[Probe] =
    probe(qFor(prefix), Some(facetsFor(window, prefix)))

  private def probe(q: String, facets: Option[String]): Option[Probe] = {
    val outcome = Try(parse(readBody(searchUrl(q, facets, 0, pageLimit)))) match {
      case Success(json) => totalOf(json).map(total => Probe(total, docsOf(json)))
      case Failure(e) =>
        logger.warn(s"$shortName: probe failed for q='$q' (${messageOf(e)})")
        None
    }
    Thread.sleep(restMillis)
    outcome
  }

  /** A count with no documents -- for the corpus tripwire, which wants the
    * number and nothing else.
    */
  private def countOnly(q: String, facets: Option[String]): Option[Int] = {
    val outcome = Try(parse(readBody(searchUrl(q, facets, 0, 1)))) match {
      case Success(json) => totalOf(json)
      case Failure(e) =>
        logger.warn(s"$shortName: count failed for q='$q' (${messageOf(e)})")
        None
    }
    Thread.sleep(restMillis)
    outcome
  }

  /** The `q` clause: the hub's base query, or a prefix clause when partitioning. */
  private def qFor(prefix: String): String =
    if (prefix.isEmpty) baseQuery else s"$partitionField,begins_with,$prefix"

  /** Facet filters. A non-empty prefix has displaced the base query out of `q`,
    * so any facet form of it must be restated here.
    */
  private def facetsFor(window: String, prefix: String): String = {
    val clauses =
      (if (prefix.isEmpty) Seq.empty else partitionBaseFacets) :+
        s"$facetName,include,$window"
    clauses.mkString(FacetJoin)
  }

  private def searchUrl(
      q: String,
      facets: Option[String],
      offset: Int,
      limit: Int
  ): URL = {
    val params = viewParams ++ Seq("q" -> q) ++
      facets.map("multiFacets" -> _).toSeq ++
      Seq(
        "offset" -> offset.toString,
        "limit" -> limit.toString,
        "apikey" -> apiKey
      )
    new URL(
      s"$endpoint?" + params
        .map { case (k, v) => s"${encode(k)}=${encode(v)}" }
        .mkString("&")
    )
  }

  /** One GET, no hidden retries.
    *
    * [[dpla.ingestion3.utils.HttpUtils.execute]] is the project's non-retrying
    * send -- the retries live in `makeGetRequest`, which is the wrong contract
    * here: a retry would multiply a refused request against the partner's
    * endpoint and blur a hard refusal into a slow failure. The request is built
    * locally only for the longer read timeout.
    */
  private def readBody(url: URL): String =
    HttpUtils.execute(
      HttpRequest
        .newBuilder()
        .uri(URI.create(url.toString))
        .timeout(Duration.ofSeconds(60))
        .GET()
        .build()
    )

  /** Filenames of every id file this hub has written, newest-last by name.
    *
    * Listed once per run: both the known-id set and the gap warning's "when did
    * we last run" need this directory, and walking it twice for one filename is
    * waste that grows with the history.
    */
  private[api] def idFileNames(): Seq[String] =
    if (!Files.isDirectory(discoveryDir)) Seq.empty
    else
      Try {
        val stream = Files.list(discoveryDir)
        try stream.iterator().asScala.map(_.getFileName.toString).toVector
        finally stream.close()
      }.getOrElse(Seq.empty)

  /** Every id this hub has discovered before, so a run reports only what is new.
    *
    * Reads the whole history each run. At a few thousand ids a week that is
    * seconds and tens of MB after a year, so it is left simple -- but it is
    * O(every id ever discovered), with no ceiling. If that ever bites, write a
    * compacted set beside the dated files and read only that plus anything
    * newer; the dated files stay for audit and for the S3 sync.
    */
  private[api] def loadKnownIds(names: Seq[String]): Set[String] =
    names
      .filter(_.endsWith(IdFileSuffix))
      .flatMap { name =>
        Try(parseIdFile(Files.readAllLines(discoveryDir.resolve(name)).asScala.iterator))
          .getOrElse(Seq.empty)
      }
      .toSet

  private def writeIds(ids: Seq[String], now: LocalDateTime): Unit =
    if (ids.isEmpty)
      logger.info(s"$shortName: no previously unknown ids this run; nothing written")
    else {
      Files.createDirectories(discoveryDir)
      val path = discoveryDir.resolve(now.format(FileTimestamp) + IdFileSuffix)
      Files.write(path, ids.asJava, StandardCharsets.UTF_8)
      logger.info(s"$shortName: wrote ${ids.size} new id(s) to $path")
    }
}

/** A slice's row count and the first page of it, fetched together. */
private[api] case class Probe(total: Int, docs: Seq[JValue])

object PrimoIdDiscovery {

  val DefaultEndpoint = "https://api-na.hosted.exlibrisgroup.com/primo/v1/search"
  val DefaultFacet = "facet_newrecords"

  /** Primo's windows are cumulative -- 07 is a subset of 30, which is a subset
    * of 90 -- so only the widest is ever worth asking for. Listing them all lets
    * a tenant that exposes different values override this wholesale.
    */
  val DefaultWindows = "07 days back,30 days back,90 days back"

  val DefaultPartitionField = "title"
  val DefaultAlphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
  val DefaultRestSeconds = 1.0
  val DefaultMaxDepth = 6

  val DiscoveryDirName = "discovery"
  val IdFileSuffix = ".ids"

  /** Multiple facet clauses are joined by `|,|`, not `&` or a bare comma. */
  val FacetJoin = "|,|"

  def encode(s: String): String =
    URLEncoder.encode(s, StandardCharsets.UTF_8.name())

  /** Distinct ids carried by a page of results, in the order they appear.
    *
    * Primo groups related holdings, so the same `recordid` can occupy several
    * rows of one result set -- `title,begins_with,q` returns two rows for one
    * record. Collapsing them here is correct; what must NOT be done is to read
    * the smaller distinct count as evidence that records were missed, because
    * `info.total` counts rows. Completeness is measured in rows, by the caller.
    */
  def idsFrom(docs: Seq[JValue]): Seq[String] =
    docs.flatMap(recordIdOf).distinct

  val FileTimestamp: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")

  private val IdFileStamp = """^(\d{8})_\d{6}\.ids$""".r

  /** Splits a comma-delimited config value, trimming and dropping blanks. */
  def splitList(raw: String): Seq[String] =
    Option(raw).getOrElse("").split(",").map(_.trim).filter(_.nonEmpty).toSeq

  /** Splits a config value into single-character partition prefixes. */
  def splitChars(raw: String): Seq[String] =
    Option(raw).getOrElse("").trim.toSeq.map(_.toString).filter(_.trim.nonEmpty)

  /** Parses `vid=DPLA,tab=dpla,scope=DPLA` into request parameters.
    *
    * A flat string rather than a config list: these are always simple key/value
    * pairs, and HOCON lists of objects make the conf file harder to read for the
    * one person who edits it.
    */
  def parseViewParams(raw: String): Seq[(String, String)] =
    splitList(raw).flatMap { pair =>
      pair.split("=", 2) match {
        case Array(k, v) if k.trim.nonEmpty => Some(k.trim -> v.trim)
        case _                              => None
      }
    }

  /** Days covered by a window value such as `90 days back`. */
  def windowDays(window: String): Option[Long] =
    """^\s*(\d+)\s*days?\s*back\s*$""".r
      .findFirstMatchIn(Option(window).getOrElse(""))
      .flatMap(m => Try(m.group(1).toLong).toOption)

  /** The window reaching furthest back. Values that do not parse are ignored
    * rather than guessed at, so a malformed entry cannot silently win.
    */
  def widestWindow(windows: Seq[String]): Option[String] =
    Option(windows).getOrElse(Seq.empty).filter(w => windowDays(w).isDefined) match {
      case Nil   => Option(windows).getOrElse(Seq.empty).headOption
      case valid => Some(valid.maxBy(w => windowDays(w).getOrElse(0L)))
    }

  /** Every offset a slice is paged at, in order, for this tenant's ceiling.
    *
    * Precomputed rather than stepped. A cursor that clamps to `maxOffset` can
    * return its own current value, and a paging loop that keeps going while the
    * page is full then re-issues one request at the partner forever. A finite
    * sequence cannot do that. The last entry lands exactly on `maxOffset` so the
    * records between the final whole step and the ceiling are still reached; it
    * overlaps the previous page, and callers dedupe.
    */
  def pageOffsets(pageLimit: Int, maxOffset: Int): Seq[Int] = {
    require(pageLimit > 0, s"pageLimit must be positive (got $pageLimit)")
    require(maxOffset >= 0, s"maxOffset must not be negative (got $maxOffset)")
    ((0 to maxOffset by pageLimit) :+ maxOffset).distinct.sorted
  }

  /** The message of a throwable, falling back to its toString. */
  def messageOf(e: Throwable): String =
    Option(e.getMessage).getOrElse(e.toString)

  /** The `docs` array of a Primo search response. */
  def docsOf(json: JValue): List[JValue] = json \ "docs" match {
    case JArray(docs) => docs
    case _            => Nil
  }

  /** `info.total` -- the hub's own count for a query, independent of paging. */
  def totalOf(json: JValue): Option[Int] = json \ "info" \ "total" match {
    case org.json4s.JsonAST.JInt(n)    => Try(n.toInt).toOption
    case org.json4s.JsonAST.JLong(n)   => Try(n.toInt).toOption
    case org.json4s.JsonAST.JDouble(n) => Try(n.toInt).toOption
    case JString(s)                    => Try(s.trim.toInt).toOption
    case _                             => None
  }

  /** Extracts a record id as a plain string.
    *
    * Primo nests `recordid` under `pnx.control` and wraps it in an array. Taking
    * `.toString` of the AST node yields `JArray(List(JString(...)))` rather than
    * the id, which is harmless while ids are only written out and wrong the
    * moment they are read back as a seed.
    */
  def recordIdOf(doc: JValue): Option[String] =
    (doc \\ "control" \ "recordid") match {
      case JString(id)                 => Some(id).filter(_.nonEmpty)
      case JArray(JString(id) :: _)    => Some(id).filter(_.nonEmpty)
      case _                           => None
    }

  /** Reads an id file, ignoring blank lines and `#` comments. */
  def parseIdFile(lines: Iterator[String]): Seq[String] =
    lines
      .map(_.trim)
      .filter(l => l.nonEmpty && !l.startsWith("#"))
      .toSeq

  /** Newest id file in a listing. The timestamp is fixed-width and zero-padded,
    * so lexical order is chronological order.
    */
  def newestIdFile(names: Seq[String]): Option[String] =
    Option(names).getOrElse(Seq.empty).filter(n => IdFileStamp.pattern.matcher(n).matches()) match {
      case Nil     => None
      case matches => Some(matches.max)
    }

  def idFileDate(name: String): Option[LocalDate] =
    IdFileStamp
      .findFirstMatchIn(Option(name).getOrElse(""))
      .flatMap(m =>
        Try(LocalDate.parse(m.group(1), DateTimeFormatter.BASIC_ISO_DATE)).toOption
      )

  /** Warns when more time has passed since the last discovery than the window
    * covers, because records added in the uncovered span are in neither the
    * previous run's output nor this one's -- and no later run will find them.
    */
  def gapWarning(
      last: Option[LocalDate],
      now: LocalDate,
      windowDays: Option[Long]
  ): Option[String] = {
    val days = windowDays.getOrElse(0L)
    last match {
      case None =>
        Some(
          s"no previous discovery run found. Any record added more than $days days " +
            s"before today is outside the window and will not be found by this run."
        )
      case Some(previous) =>
        val elapsed = ChronoUnit.DAYS.between(previous, now)
        if (elapsed <= days) None
        else
          Some(
            s"$elapsed days since the last discovery run ($previous), but the window " +
              s"reaches back only $days days. Records added between $previous and " +
              s"${now.minusDays(days)} are in neither, and no later run will find them."
          )
    }
  }
}

/** One paged query: the distinct ids it yielded, and how many result rows it
  * read. Rows are what `info.total` counts, so rows are what completeness is
  * measured against.
  */
private[api] case class PagedSlice(ids: Seq[String], rows: Int)

/** What a discovery run found, and anything that stops it being complete. */
case class DiscoveryResult(
    shortName: String,
    window: String,
    windowTotal: Int,
    retrieved: Int,
    fresh: Int,
    corpusTotal: Option[Int],
    shortfalls: Seq[String]
) {
  def complete: Boolean = shortfalls.isEmpty

  def summary: String = {
    val corpus = corpusTotal.map(t => s", view holds $t").getOrElse("")
    val base =
      s"$shortName discovery: $retrieved of $windowTotal id(s) in '$window' " +
        s"($fresh previously unknown$corpus)"
    if (complete) base
    else base + s"; INCOMPLETE -- ${shortfalls.size} problem(s): ${shortfalls.mkString("; ")}"
  }
}
