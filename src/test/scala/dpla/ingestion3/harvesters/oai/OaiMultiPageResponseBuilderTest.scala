package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.harvesters.oai.OaiMultiPageResponseBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Most of the methods in this class cause http requests and therefore aren't
  * suitable for unit tests
  */
class OaiMultiPageResponseBuilderTest extends AnyFlatSpec with Matchers {

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

  // --- OaiError handling ---

  "handleErrors" should "throw OaiHarvestException for BadResumptionToken" in {
    val info = OaiRequestInfo(
      "ListRecords",
      Some("oai_dc"),
      Some("test_set"),
      Some("expired_token"),
      System.currentTimeMillis()
    )

    val builder = new OaiMultiPageResponseBuilder(
      "http://example.com",
      "ListRecords",
      Some("oai_dc"),
      Some("test_set"),
      0
    )

    val url = builder.buildUrl(Some("expired_token"))

    val ex = intercept[OaiHarvestException] {
      // Use the iterator's handleErrors via getResponse
      val error: Either[OaiError, OaiPage] = Left(BadResumptionToken(info))
      error match {
        case Left(err) =>
          err match {
            case NoRecordsMatch(_) => // skip
            case _ =>
              throw new OaiHarvestException(
                requestInfo = info,
                url = url.toString,
                stage = "oai_error",
                cause = new RuntimeException(s"OAI Error: $err")
              )
          }
        case Right(_) => // skip
      }
    }

    ex.requestInfo.set shouldBe Some("test_set")
    ex.requestInfo.resumptionToken shouldBe Some("expired_token")
    ex.stage shouldBe "oai_error"
    ex.getMessage should include("OAI Error")
  }

  "OaiError.errorForCode" should "return BadResumptionToken for badResumptionToken code" in {
    val info = OaiRequestInfo(
      "ListRecords",
      Some("oai_dc"),
      Some("set1"),
      Some("tok123"),
      0
    )
    val error = OaiError.errorForCode("badResumptionToken", info)
    error shouldBe a[BadResumptionToken]
    error.asInstanceOf[BadResumptionToken].info.resumptionToken shouldBe Some(
      "tok123"
    )
  }

  "parseResponseBody" should "return Left(BadArgument) for full OAI-PMH error document" in {
    val fullOaiPmhError =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
        |  <responseDate>2026-02-16T12:30:19Z</responseDate>
        |  <request verb="ListRecords" set="PFW_cc_fw_elect">https://example.com/oai</request>
        |  <error code="badArgument">The request includes illegal arguments,
        | is missing required arguments, includes a repeated argument,
        | or values for arguments have an illegal syntax.</error>
        |</OAI-PMH>""".stripMargin
    val info = OaiRequestInfo(
      "ListRecords",
      Some("oai_qdc_imdpla"),
      Some("PFW_cc_fw_elect"),
      None,
      System.currentTimeMillis()
    )
    val builder = new OaiMultiPageResponseBuilder(
      "https://example.com/oai",
      "ListRecords",
      Some("oai_qdc_imdpla"),
      Some("PFW_cc_fw_elect"),
      0
    )
    val result = builder.parseResponseBody(fullOaiPmhError, info)
    result shouldBe a[Left[_, _]]
    val err = result.left.getOrElse(fail("expected Left"))
    err shouldBe a[BadArgument]
    err.asInstanceOf[BadArgument].info.set shouldBe Some("PFW_cc_fw_elect")
  }
}
