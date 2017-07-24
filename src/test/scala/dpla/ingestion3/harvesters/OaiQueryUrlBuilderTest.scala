package dpla.ingestion3.harvesters

import org.scalatest._

/**
  * Tests for OaiQueryUrlBuilder
  */
class OaiQueryUrlBuilderTest extends FlatSpec {
  val builder = new OaiQueryUrlBuilder
  val endpoint = "http://repox.example.edu:8080/repox/OAIHandler"
  val verb = "ListRecords"
  val set = "manuscripts"
  val prefix = "mods"
  val token = "1234"

  "buildQueryUrl" should "throw AssertionError if endpoint is not defined" in {
    val params = Map("verb" -> verb)
    assertThrows[AssertionError](builder.buildQueryUrl(params))
  }
  it should "throw Exception if verb is not defined" in {
    val params = Map("endpoint" -> endpoint)
    assertThrows[AssertionError](builder.buildQueryUrl(params))
  }
  it should "make URL with given protocol" in {
    val params = Map("endpoint" -> endpoint, "verb" -> verb)
    val url = builder.buildQueryUrl(params)
    assert(url.getProtocol === "http")
  }
  it should "make URL with given host" in {
    val params = Map("endpoint" -> endpoint, "verb" -> verb)
    val url = builder.buildQueryUrl(params)
    assert(url.getHost === "repox.example.edu")
  }
  it should "make URL with given port" in {
    val params = Map("endpoint" -> endpoint, "verb" -> verb)
    val url = builder.buildQueryUrl(params)
    assert(url.getPort === 8080)
  }
  it should "make URL with given path" in {
    val params = Map("endpoint" -> endpoint, "verb" -> verb)
    val url = builder.buildQueryUrl(params)
    assert(url.getPath === "/repox/OAIHandler")
  }
  it should "make URL with given verb" in {
    val params = Map("endpoint" -> endpoint, "verb" -> verb)
    val url = builder.buildQueryUrl(params)
    assert(url.getQuery.contains(s"verb=$verb"))
  }
  it should "make URL with given set" in {
    val params = Map("endpoint" -> endpoint, "verb" -> verb, "set" -> set)
    val url = builder.buildQueryUrl(params)
    assert(url.getQuery.contains(s"set=$set"))
  }
  it should "make URL with given metadataPrefix" in {
    val params = Map("endpoint" -> endpoint, "verb" -> verb, "metadataPrefix" -> prefix)
    val url = builder.buildQueryUrl(params)
    assert(url.getQuery.contains(s"metadataPrefix=$prefix"))
  }
  it should "make URL with given resumptionToken" in {
    val params = Map("endpoint" -> endpoint, "verb" -> verb, "resumptionToken" -> token)
    val url = builder.buildQueryUrl(params)
    assert(url.getQuery.contains(s"resumptionToken=$token"))
  }
}
