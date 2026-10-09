package dpla.ingestion3.harvesters.api

import org.json4s.jackson.JsonMethods.parse
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalDate

class PrimoIdDiscoveryTest extends AnyFlatSpec with Matchers {

  import PrimoIdDiscovery._

  // -- config parsing ---------------------------------------------------------

  "splitList" should "trim entries and drop blanks" in {
    splitList(" a , b ,, c ") shouldBe Seq("a", "b", "c")
  }

  it should "return empty for null or blank input" in {
    splitList(null) shouldBe Seq.empty
    splitList("") shouldBe Seq.empty
    splitList("  ,  ") shouldBe Seq.empty
  }

  "splitChars" should "split a charset into single-character prefixes" in {
    splitChars("abc0") shouldBe Seq("a", "b", "c", "0")
  }

  it should "tolerate null" in {
    splitChars(null) shouldBe Seq.empty
  }

  "parseViewParams" should "parse key=value pairs" in {
    parseViewParams("vid=DPLA,tab=dpla,scope=DPLA") shouldBe
      Seq("vid" -> "DPLA", "tab" -> "dpla", "scope" -> "DPLA")
  }

  it should "keep values containing an equals sign" in {
    parseViewParams("q=a=b") shouldBe Seq("q" -> "a=b")
  }

  it should "drop entries with no key" in {
    parseViewParams("=DPLA,vid=X") shouldBe Seq("vid" -> "X")
  }

  it should "return empty for blank input" in {
    parseViewParams("") shouldBe Seq.empty
  }

  // -- windows ----------------------------------------------------------------

  "windowDays" should "read the leading integer" in {
    windowDays("90 days back") shouldBe Some(90L)
    windowDays("07 days back") shouldBe Some(7L)
    windowDays("1 day back") shouldBe Some(1L)
  }

  it should "tolerate surrounding whitespace" in {
    windowDays("  30 days back  ") shouldBe Some(30L)
  }

  it should "return None for values it cannot read" in {
    windowDays("last quarter") shouldBe None
    windowDays("") shouldBe None
    windowDays(null) shouldBe None
  }

  "widestWindow" should "pick the window reaching furthest back" in {
    widestWindow(Seq("07 days back", "90 days back", "30 days back")) shouldBe
      Some("90 days back")
  }

  it should "ignore values it cannot parse rather than let them win" in {
    widestWindow(Seq("07 days back", "whenever", "30 days back")) shouldBe
      Some("30 days back")
  }

  it should "fall back to the first entry when none parse" in {
    widestWindow(Seq("whenever", "someday")) shouldBe Some("whenever")
  }

  it should "return None for an empty list" in {
    widestWindow(Seq.empty) shouldBe None
    widestWindow(null) shouldBe None
  }

  // -- paging -----------------------------------------------------------------

  "pageOffsets" should "step by the page limit and finish on the ceiling" in {
    // 0 -> 1000 -> 2000 would overshoot a 1999 cap and skip records 2000-2998,
    // which the ceiling page does reach.
    pageOffsets(1000, 1999) shouldBe Seq(0, 1000, 1999)
  }

  it should "produce an exact sequence when the limit divides the ceiling" in {
    // Mississippi: offset + limit <= 500
    pageOffsets(100, 400) shouldBe Seq(0, 100, 200, 300, 400)
  }

  it should "never repeat an offset" in {
    // A repeated offset is what lets a paging loop re-issue one request forever
    // when the page at the ceiling comes back full.
    Seq((1000, 1999), (100, 400), (50, 50), (1000, 0), (7, 20)).foreach {
      case (limit, max) =>
        val offsets = pageOffsets(limit, max)
        withClue(s"limit=$limit max=$max -> $offsets: ") {
          offsets shouldBe offsets.distinct
          offsets shouldBe offsets.sorted
          offsets.last shouldBe max
        }
    }
  }

  it should "yield a single offset when the ceiling is zero" in {
    pageOffsets(100, 0) shouldBe Seq(0)
  }

  it should "reject a non-positive page limit rather than loop" in {
    an[IllegalArgumentException] should be thrownBy pageOffsets(0, 100)
    an[IllegalArgumentException] should be thrownBy pageOffsets(-1, 100)
  }

  "messageOf" should "fall back to toString when there is no message" in {
    messageOf(new RuntimeException("boom")) shouldBe "boom"
    messageOf(new RuntimeException()) should include("RuntimeException")
  }

  // -- response parsing -------------------------------------------------------

  "docsOf" should "return the docs array" in {
    docsOf(parse("""{"docs":[{"a":1},{"a":2}]}""")).size shouldBe 2
  }

  it should "return empty when docs is absent or not an array" in {
    docsOf(parse("""{"info":{"total":5}}""")) shouldBe Nil
    docsOf(parse("""{"docs":"nope"}""")) shouldBe Nil
  }

  "totalOf" should "read an integer total" in {
    totalOf(parse("""{"info":{"total":2438}}""")) shouldBe Some(2438)
  }

  it should "read a total delivered as a string" in {
    totalOf(parse("""{"info":{"total":"2438"}}""")) shouldBe Some(2438)
  }

  it should "return None when the total is missing" in {
    totalOf(parse("""{"docs":[]}""")) shouldBe None
    totalOf(parse("""{"info":{}}""")) shouldBe None
  }

  it should "return None rather than zero for an unreadable total" in {
    // A missing count must never be mistaken for an empty window: that would
    // silently report a complete run having asked for nothing.
    totalOf(parse("""{"info":{"total":"many"}}""")) shouldBe None
  }

  "recordIdOf" should "extract an id nested under control" in {
    val doc = parse("""{"pnx":{"control":{"recordid":["alma991014112361805566"]}}}""")
    recordIdOf(doc) shouldBe Some("alma991014112361805566")
  }

  it should "extract an id that is not wrapped in an array" in {
    val doc = parse("""{"pnx":{"control":{"recordid":"GETTY_ROSETTAIE10176553"}}}""")
    recordIdOf(doc) shouldBe Some("GETTY_ROSETTAIE10176553")
  }

  it should "return None when there is no recordid" in {
    recordIdOf(parse("""{"pnx":{"control":{}}}""")) shouldBe None
    recordIdOf(parse("""{}""")) shouldBe None
  }

  it should "treat an empty id as absent" in {
    recordIdOf(parse("""{"pnx":{"control":{"recordid":[""]}}}""")) shouldBe None
  }

  "idsFrom" should "collapse rows that carry the same record" in {
    // Primo groups related holdings, so one record can occupy several rows.
    // Verified live: `title,begins_with,q` returns info.total = 2 and two docs
    // that are the same record. Reading the smaller distinct count as a
    // shortfall would report phantom loss on essentially every slice.
    val docs = Seq(
      parse("""{"pnx":{"control":{"recordid":["alma991015110953105566"]}}}"""),
      parse("""{"pnx":{"control":{"recordid":["alma991015110953105566"]}}}""")
    )
    idsFrom(docs) shouldBe Seq("alma991015110953105566")
  }

  it should "preserve order and drop docs with no id" in {
    val docs = Seq(
      parse("""{"pnx":{"control":{"recordid":["b"]}}}"""),
      parse("""{"pnx":{"control":{}}}"""),
      parse("""{"pnx":{"control":{"recordid":["a"]}}}""")
    )
    idsFrom(docs) shouldBe Seq("b", "a")
  }

  it should "return empty for no docs" in {
    idsFrom(Seq.empty) shouldBe Seq.empty
  }

  // -- id files ---------------------------------------------------------------

  "parseIdFile" should "read ids, skipping blanks and comments" in {
    val lines = Iterator("# written by discovery", "", "  id1  ", "id2", "   ")
    parseIdFile(lines) shouldBe Seq("id1", "id2")
  }

  "newestIdFile" should "pick the latest by lexical order" in {
    val names = Seq("20260901_120000.ids", "20260918_090000.ids", "20260910_235959.ids")
    newestIdFile(names) shouldBe Some("20260918_090000.ids")
  }

  it should "ignore files that are not id files" in {
    newestIdFile(Seq("notes.txt", "_STATE.json", "20260901_120000.ids")) shouldBe
      Some("20260901_120000.ids")
  }

  it should "return None when nothing matches" in {
    newestIdFile(Seq("notes.txt")) shouldBe None
    newestIdFile(Seq.empty) shouldBe None
  }

  "idFileDate" should "read the date out of the filename" in {
    idFileDate("20260918_090000.ids") shouldBe Some(LocalDate.of(2026, 9, 18))
  }

  it should "return None for a name it cannot read" in {
    idFileDate("latest.ids") shouldBe None
    idFileDate("") shouldBe None
  }

  // -- the gap ----------------------------------------------------------------

  "gapWarning" should "stay silent when the window still covers the interval" in {
    gapWarning(Some(LocalDate.of(2026, 9, 11)), LocalDate.of(2026, 9, 18), Some(90L)) shouldBe None
  }

  it should "stay silent at exactly the window edge" in {
    gapWarning(Some(LocalDate.of(2026, 6, 20)), LocalDate.of(2026, 9, 18), Some(90L)) shouldBe None
  }

  it should "warn once the interval exceeds the window" in {
    val warning =
      gapWarning(Some(LocalDate.of(2026, 1, 1)), LocalDate.of(2026, 9, 18), Some(90L))
    warning should not be empty
    warning.get should include("2026-01-01")
    warning.get should include("260 days")
  }

  it should "name the span whose records are unrecoverable" in {
    val warning =
      gapWarning(Some(LocalDate.of(2026, 1, 1)), LocalDate.of(2026, 9, 18), Some(90L))
    // Everything between the last run and the far edge of the window is lost.
    warning.get should include("2026-06-20")
  }

  it should "warn when there is no previous run at all" in {
    val warning = gapWarning(None, LocalDate.of(2026, 9, 18), Some(90L))
    warning should not be empty
    warning.get should include("no previous discovery run")
  }

  // -- results ----------------------------------------------------------------

  private def result(shortfalls: Seq[String]) = DiscoveryResult(
    shortName = "mississippi",
    window = "90 days back",
    windowTotal = 2438,
    retrieved = 2438,
    fresh = 12,
    corpusTotal = Some(117321),
    shortfalls = shortfalls
  )

  "DiscoveryResult" should "be complete only when nothing fell short" in {
    result(Seq.empty).complete shouldBe true
    result(Seq("prefix 'z' returned 4 of 9")).complete shouldBe false
  }

  it should "summarise counts on a clean run" in {
    val s = result(Seq.empty).summary
    s should include("2438 id(s)")
    s should include("12 previously unknown")
    s should include("view holds 117321")
    s should not include "INCOMPLETE"
  }

  it should "say so loudly when incomplete" in {
    val s = result(Seq("prefix 'z' returned 4 of 9")).summary
    s should include("INCOMPLETE")
    s should include("prefix 'z' returned 4 of 9")
  }

}
