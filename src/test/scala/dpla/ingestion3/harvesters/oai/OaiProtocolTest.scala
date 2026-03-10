package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.harvesters.oai.{OaiConfiguration, OaiPage, OaiProtocol, OaiRequestInfo}
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OaiProtocolTest extends AnyFlatSpec with Matchers {

  val oaiProtocol = new OaiProtocol(
    OaiConfiguration(Map("verb" -> "ListRecords"))
  )
  val flatFileIO = new FlatFileIO()

  "An OaiProtocol" should "parse a records page into records" in {
    val oaiXmlString = flatFileIO.readFileAsString("/oai-page.xml")
    val info = OaiRequestInfo("verb", Some("metadataPrefix"), Some("set"), Some("token"), System.currentTimeMillis())
    val result = oaiProtocol.parsePageIntoRecords(
      OaiPage(oaiXmlString, info),
      removeDeleted = false
    )
    assert(Seq(result).nonEmpty)
  }

  it should "parse a set page into sets" in {
    val oaiXmlString = flatFileIO.readFileAsString("/oai-sets.xml")
    val info = OaiRequestInfo("verb", Some("metadataPrefix"), Some("set"), Some("token"), System.currentTimeMillis())
    val result = oaiProtocol.parsePageIntoSets(OaiPage(oaiXmlString, info))
    assert(Seq(result).nonEmpty)
  }

  // --- Invalid XML wrapping ---
  // These tests use a separate OaiProtocol with a dummy endpoint that won't
  // be contacted — we only exercise parsePageIntoRecords/parsePageIntoSets.
  private val testProtocol = new OaiProtocol(
    OaiConfiguration(Map("verb" -> "ListRecords", "endpoint" -> "http://localhost:0/oai"))
  ) {
    // Override the lazy endpoint so it doesn't validate reachability
    override lazy val endpoint: String = "http://localhost:0/oai"
  }

  it should "throw OaiHarvestException with correct context for invalid XML in parsePageIntoRecords" in {
    val badXml = "<OAI-PMH><ListRecords><record><header><identifier>oai:test:001</identifier></header>unclosed"
    val info = OaiRequestInfo("ListRecords", Some("oai_dc"), Some("bad_set"),
      Some("token_abc"), System.currentTimeMillis(), cursor = Some(42), completeListSize = Some(999))

    val ex = intercept[OaiHarvestException] {
      testProtocol.parsePageIntoRecords(OaiPage(badXml, info), removeDeleted = false)
    }

    ex.requestInfo.set shouldBe Some("bad_set")
    ex.requestInfo.resumptionToken shouldBe Some("token_abc")
    ex.stage shouldBe "page_parse"
    ex.firstId shouldBe Some("oai:test:001")
    ex.getMessage should include("Set: bad_set")
    ex.getMessage should include("Resumption token: token_abc")
  }

  it should "throw OaiHarvestException for invalid XML in parsePageIntoSets" in {
    val badXml = "<OAI-PMH><ListSets><set><setSpec>broken"
    val info = OaiRequestInfo("ListSets", None, None, Some("set_token"), System.currentTimeMillis())

    val ex = intercept[OaiHarvestException] {
      testProtocol.parsePageIntoSets(OaiPage(badXml, info))
    }

    ex.stage shouldBe "set_parse"
    ex.requestInfo.resumptionToken shouldBe Some("set_token")
  }
}
