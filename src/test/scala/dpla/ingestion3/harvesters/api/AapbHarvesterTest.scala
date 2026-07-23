package dpla.ingestion3.harvesters.api

import org.apache.http.client.utils.URIBuilder
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.flatspec.AnyFlatSpec

import scala.jdk.CollectionConverters._

/** TEST HUB — see docs/ingestion/README_TEST_HUBS.md
  *
  * Unit tests for the pure request-building / response-parsing logic of the AAPB
  * Solr cursorMark harvester (no Spark/network required).
  */
class AapbHarvesterTest extends AnyFlatSpec {

  implicit val formats: Formats = DefaultFormats

  it should "OR the access_types into a single Solr filter query" in
    assert(
      AapbHarvester.accessFilter(AapbHarvester.DefaultAccessTypes) ===
        "access_types:digitized OR access_types:online OR access_types:on-location"
    )

  it should "build a Solr URL with cursorMark, deterministic sort, and inline PBCore" in {
    val url = AapbHarvester.buildUrl(
      endpoint = AapbHarvester.DefaultEndpoint,
      query = "*",
      rows = "500",
      filterQuery = AapbHarvester.accessFilter(AapbHarvester.DefaultAccessTypes),
      cursorMark = "*"
    )
    assert(url.getHost === "americanarchive.org")
    assert(url.getPath === "/api.json")

    val params = new URIBuilder(url.toURI).getQueryParams.asScala
      .map(p => p.getName -> p.getValue)
      .toMap
    assert(params("fl") === "id,xml")
    assert(params("sort") === "id asc")
    assert(params("cursorMark") === "*")
    assert(params("rows") === "500")
    assert(params("q") === "*")
    assert(params("fq") === "access_types:digitized OR access_types:online OR access_types:on-location")
  }

  it should "extract records and the nextCursorMark from a Solr response" in {
    val json = parse(
      """{"nextCursorMark":"AoE1abc","response":{"numFound":3,"docs":[
        |  {"id":"cpb-aacip-1","xml":"<pbcoreDescriptionDocument>one</pbcoreDescriptionDocument>"},
        |  {"id":"cpb-aacip-2","xml":["<pbcoreDescriptionDocument>two</pbcoreDescriptionDocument>"]},
        |  {"id":"cpb-aacip-3"}
        |]}}""".stripMargin
    )
    val (records, next) = AapbHarvester.parsePage(json)

    assert(next === "AoE1abc")
    // The third doc has no xml and is skipped; the second's xml arrives as an array.
    assert(records.map(_.id) === List("cpb-aacip-1", "cpb-aacip-2"))
    assert(records.head.document.contains("one"))
    assert(records(1).document.contains("two"))
  }

  it should "return an empty nextCursorMark when the response omits it" in {
    val json = parse("""{"response":{"numFound":0,"docs":[]}}""")
    val (records, next) = AapbHarvester.parsePage(json)
    assert(records.isEmpty)
    assert(next === "")
  }
}
