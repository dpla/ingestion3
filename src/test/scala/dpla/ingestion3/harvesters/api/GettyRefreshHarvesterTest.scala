package dpla.ingestion3.harvesters.api

import org.json4s.jackson.JsonMethods.parse
import org.scalatest.flatspec.AnyFlatSpec

/** Unit tests for the pure parts of [[GettyRefreshHarvester]].
  *
  * Everything load-bearing is in the companion object precisely so it can be
  * exercised without a SparkSession or a network: URL construction, id
  * extraction, the route-loss signal, and seed parsing.
  */
class GettyRefreshHarvesterTest extends AnyFlatSpec {

  import GettyRefreshHarvester._

  // ── lookupUrl ──────────────────────────────────────────────────────────────

  "lookupUrl" should "query a single record by id" in {
    val url = lookupUrl("KEY", "GETTY_ROSETTAIE10176553").toString
    // rid,exact returns exactly one record, which is the whole point: no offset
    // is involved, so the gateway's offset<=1999 cap cannot apply.
    assert(url.contains("q=rid%2Cexact%2CGETTY_ROSETTAIE10176553"))
    assert(url.contains("limit=1"))
    assert(url.contains("offset=0"))
  }

  it should "target the DPLA view" in {
    val url = lookupUrl("KEY", "GETTY_OCPFL1677576").toString
    assert(url.contains("vid=DPLA"))
    assert(url.contains("tab=dpla"))
    assert(url.contains("scope=DPLA"))
    assert(url.contains("inst=01GRI"))
  }

  it should "carry the api key" in {
    assert(lookupUrl("s3cr3t", "GETTY_OCPFL1").toString.contains("apikey=s3cr3t"))
  }

  it should "encode exactly once" in {
    // Double-encoding is the failure mode that bit the Python prototype: it
    // returns a WRONG COUNT rather than an error, so it fails silently.
    val url = lookupUrl("KEY", "GETTY_ROSETTAIE1").toString
    assert(url.contains("%2C"), "comma should be encoded")
    assert(!url.contains("%252C"), "comma must not be double-encoded")
  }

  it should "not be defeated by an id containing url metacharacters" in {
    val url = lookupUrl("KEY", "GETTY_X&limit=1000&q=anything").toString
    assert(!url.contains("&limit=1000"), "id must not inject query parameters")
    assert(url.contains("limit=1"))
  }

  // ── newRecordsUrl ──────────────────────────────────────────────────────────

  "newRecordsUrl" should "filter on Getty's newrecords facet" in {
    val url = newRecordsUrl("KEY", WidestNewRecordsWindow, 0, PageLimit).toString
    assert(url.contains("multiFacets=facet_newrecords%2Cinclude%2C90+days+back"))
  }

  it should "stay inside the DPLA facet" in {
    // Discovery must not wander outside what Getty publishes to DPLA.
    val url = newRecordsUrl("KEY", WidestNewRecordsWindow, 0, PageLimit).toString
    assert(url.contains("q=facet_local5%2Cexact%2CDPLA"))
  }

  it should "page within the gateway's ceiling" in {
    val url = newRecordsUrl("KEY", WidestNewRecordsWindow, MaxOffset, PageLimit).toString
    assert(url.contains(s"offset=$MaxOffset"))
    // offset 1999 + limit 1000 reaches record 2,998 -- one past that is refused.
    assert(MaxOffset <= 1999 && PageLimit <= 1000)
  }

  "the newrecords window" should "be the widest Getty offers" in {
    // Getty's windows are cumulative (07 ⊂ 30 ⊂ 90), so only the widest is worth
    // requesting -- and 90 days is the longest that exists. A quarterly schedule
    // therefore has no margin: anything older than the window and not already
    // seeded is invisible to discovery.
    assert(WidestNewRecordsWindow === "90 days back")
  }

  // ── recordIdOf ─────────────────────────────────────────────────────────────

  "recordIdOf" should "extract the id from Primo's array-wrapped control block" in {
    val doc = parse("""{"pnx":{"control":{"recordid":["GETTY_ROSETTAIE10176553"]}}}""")
    assert(recordIdOf(doc) === Some("GETTY_ROSETTAIE10176553"))
  }

  it should "extract a bare string id" in {
    val doc = parse("""{"pnx":{"control":{"recordid":"GETTY_OCPFL1677576"}}}""")
    assert(recordIdOf(doc) === Some("GETTY_OCPFL1677576"))
  }

  it should "return a plain string, never the json4s AST's toString" in {
    // PrimoVEHarvester does `(doc \\ "control" \ "recordid").toString`, which
    // yields `JArray(List(JString(...)))`. Harmless while ids are only written
    // out; wrong the moment a harvest's ids are read back as a seed.
    val doc = parse("""{"pnx":{"control":{"recordid":["GETTY_OCPFL1"]}}}""")
    val id = recordIdOf(doc).get
    assert(!id.contains("JString"))
    assert(!id.contains("JArray"))
    assert(!id.contains("List("))
  }

  it should "return None when the id is missing or empty" in {
    assert(recordIdOf(parse("""{"pnx":{"control":{}}}""")) === None)
    assert(recordIdOf(parse("""{"pnx":{"control":{"recordid":[""]}}}""")) === None)
    assert(recordIdOf(parse("""{}""")) === None)
  }

  // ── docsOf ─────────────────────────────────────────────────────────────────

  "docsOf" should "return the docs array" in {
    val json = parse("""{"info":{"total":1},"docs":[{"a":1},{"b":2}]}""")
    assert(docsOf(json).size === 2)
  }

  it should "treat a response with no docs as empty, not an error" in {
    // This is how a withdrawn record reports itself, and it must be
    // distinguishable from a failure: one means "retire it", the other "retry".
    assert(docsOf(parse("""{"info":{"total":0},"docs":[]}""")).isEmpty)
    assert(docsOf(parse("""{"info":{"total":0}}""")).isEmpty)
  }

  // ── isForbidden ────────────────────────────────────────────────────────────

  "isForbidden" should "recognise the gateway refusing our egress" in {
    assert(isForbidden("Code: 403 Message: FORBIDDEN"))
    assert(isForbidden("HTTP requests from IP address 52.2.32.179 are not allowed"))
    assert(isForbidden("forbidden"))
  }

  it should "not mistake ordinary failures for a lost route" in {
    // Miscounting these would abort a healthy harvest partway through.
    assert(!isForbidden("Connection reset"))
    assert(!isForbidden("Code: 500 Message: Internal Server Error"))
    assert(!isForbidden("java.net.SocketTimeoutException: read timed out"))
    assert(!isForbidden(""))
    assert(!isForbidden(null))
  }

  // ── parseIdFile ────────────────────────────────────────────────────────────

  "parseIdFile" should "read one id per line" in {
    val ids = parseIdFile(Iterator("GETTY_OCPFL1", "GETTY_OCPFL2", "GETTY_ROSETTAIE3"))
    assert(ids === Seq("GETTY_OCPFL1", "GETTY_OCPFL2", "GETTY_ROSETTAIE3"))
  }

  it should "ignore blanks, whitespace and comments" in {
    val ids = parseIdFile(
      Iterator("# seed generated 2026-09-12", "", "  GETTY_OCPFL1  ", "   ", "GETTY_OCPFL2")
    )
    assert(ids === Seq("GETTY_OCPFL1", "GETTY_OCPFL2"))
  }

  it should "deduplicate while preserving order" in {
    // The 2021 seed held 100,602 lines but only 100,110 distinct ids; looking
    // the duplicates up again is pure waste against a partner's endpoint.
    val ids = parseIdFile(Iterator("A", "B", "A", "C", "B"))
    assert(ids === Seq("A", "B", "C"))
  }

  it should "return empty for an empty file" in {
    assert(parseIdFile(Iterator.empty).isEmpty)
  }

  // ── guard rails ────────────────────────────────────────────────────────────

  "the route-loss threshold" should "be small enough to protect the partner" in {
    // The whole point is to stop early rather than work through ~101K ids
    // against an endpoint that has already refused us.
    assert(RouteLostAfter > 0 && RouteLostAfter <= 5)
  }

  "the resolved-fraction floor" should "block publishing a collapsed harvest" in {
    assert(MinResolvedFraction > 0.0 && MinResolvedFraction <= 1.0)
  }
}
