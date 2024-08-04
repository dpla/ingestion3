package dpla.ingestion3.harvesters.oai.refactor

import org.scalatest.flatspec.AnyFlatSpec


/** Most of the methods in this class cause http requests and therefore aren't
  * suitable for unit tests
  */
class OaiMultiPageResponseBuilderTest extends AnyFlatSpec {

  "An OaiMultiPageResponseBuilder" should "build a URL without a resumption token" in {
    val oaiMultiPageResponseBuilder = new OaiMultiPageResponseBuilder(
      "http://example.com",
      "ListRecords",
      Some("oai_dc"),
      None,
      0
    )
    val url = oaiMultiPageResponseBuilder.buildUrl()
    val query = url.getQuery
    assert(
      url.getHost === "example.com"
    )
    assert(query.contains("verb=ListRecords"))
    assert(query.contains("metadataPrefix=oai_dc"))
  }

  it should "build a URL with a resumption token" in {
    val oaiMultiPageResponseBuilder = new OaiMultiPageResponseBuilder(
      "http://example.com",
      "ListRecords",
      Some("oai_dc"),
      None,
      0
    )
    val url = oaiMultiPageResponseBuilder.buildUrl(Some("token"))
    val query = url.getQuery
    assert(
      url.getHost === "example.com"
    )
    assert(query.contains("verb=ListRecords"))
    assert(query.contains("resumptionToken=token"))
  }
}
