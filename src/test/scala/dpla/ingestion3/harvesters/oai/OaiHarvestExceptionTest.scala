package dpla.ingestion3.harvesters.oai

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OaiHarvestExceptionTest extends AnyFlatSpec with Matchers {

  private val sampleInfo = OaiRequestInfo(
    verb = "ListRecords",
    metadataPrefix = Some("oai_dc"),
    set = Some("problematic_set"),
    resumptionToken = Some("abc123token"),
    timestamp = System.currentTimeMillis(),
    cursor = Some(500),
    completeListSize = Some(1200)
  )

  private val sampleUrl =
    "https://example.com/oai?verb=ListRecords&resumptionToken=abc123token"
  private val sampleCause =
    new org.xml.sax.SAXParseException("Invalid character", null)

  "OaiHarvestException" should "include set in message" in {
    val ex = new OaiHarvestException(
      sampleInfo,
      sampleUrl,
      "page_parse",
      cause = sampleCause
    )
    ex.getMessage should include("Set: problematic_set")
  }

  it should "include resumption token in message" in {
    val ex = new OaiHarvestException(
      sampleInfo,
      sampleUrl,
      "page_parse",
      cause = sampleCause
    )
    ex.getMessage should include("Resumption token: abc123token")
  }

  it should "include cursor and total in message" in {
    val ex = new OaiHarvestException(
      sampleInfo,
      sampleUrl,
      "page_parse",
      cause = sampleCause
    )
    ex.getMessage should include("Cursor: 500")
    ex.getMessage should include("/ 1200")
  }

  it should "include stage in message" in {
    val ex = new OaiHarvestException(
      sampleInfo,
      sampleUrl,
      "page_parse",
      cause = sampleCause
    )
    ex.getMessage should include("stage page_parse")
  }

  it should "include URL in message" in {
    val ex = new OaiHarvestException(
      sampleInfo,
      sampleUrl,
      "page_parse",
      cause = sampleCause
    )
    ex.getMessage should include(sampleUrl)
  }

  it should "include cause class and message" in {
    val ex = new OaiHarvestException(
      sampleInfo,
      sampleUrl,
      "page_parse",
      cause = sampleCause
    )
    ex.getMessage should include("SAXParseException")
    ex.getMessage should include("Invalid character")
  }

  it should "include first and last identifiers when provided" in {
    val ex = new OaiHarvestException(
      sampleInfo,
      sampleUrl,
      "page_parse",
      firstId = Some("oai:example:001"),
      lastId = Some("oai:example:050"),
      cause = sampleCause
    )
    ex.getMessage should include("First identifier: oai:example:001")
    ex.getMessage should include("Last identifier: oai:example:050")
  }

  it should "preserve the original cause" in {
    val ex = new OaiHarvestException(
      sampleInfo,
      sampleUrl,
      "page_parse",
      cause = sampleCause
    )
    ex.getCause shouldBe sampleCause
  }

  it should "format OAI error (oai_error stage) with human-readable labels and OriginalMessage" in {
    val info = OaiRequestInfo(
      "ListRecords",
      Some("oai_qdc_imdpla"),
      Some("PFW_cc_fw_elect"),
      None,
      1771279252935L
    )
    val oaiErr = BadArgument(info)
    val rawCause = new RuntimeException(s"OAI Error: $oaiErr")
    val ex = new OaiHarvestException(
      info,
      "https://example.com/oai",
      "oai_error",
      oaiError = Some(oaiErr),
      cause = rawCause
    )
    val msg = ex.getMessage
    msg should include("Cause: RuntimeException: OAI Error")
    msg should include("OAI Error: BadArgument")
    msg should include("MetadataPrefix: oai_qdc_imdpla")
    msg should include("OAI Set: PFW_cc_fw_elect")
    msg should include("Resumption Token: -")
    msg should include("OriginalMessage:")
    msg should include("OAI Error: BadArgument")
  }

  "OaiHarvestException.formatForSlack" should "produce Slack mrkdwn" in {
    val msg = OaiHarvestException.formatForSlack(
      sampleInfo,
      sampleUrl,
      "page_parse",
      Some("oai:example:001"),
      Some("oai:example:050"),
      sampleCause
    )
    msg should include("*OAI debug context*")
    msg should include("• Set: `problematic_set`")
    msg should include("• Resumption token: `abc123token`")
    msg should include("• Cursor: 500 / 1200")
    msg should include(sampleUrl)
    msg should include("*Error*:")
  }

  "OaiHarvestException.formatForEmail" should "produce plain text" in {
    val msg = OaiHarvestException.formatForEmail(
      sampleInfo,
      sampleUrl,
      "page_parse",
      Some("oai:example:001"),
      Some("oai:example:050"),
      sampleCause
    )
    msg should include("OAI debug context:")
    msg should include("- Set: problematic_set")
    msg should include("- Resumption token: abc123token")
    msg should include("- Cursor: 500 / 1200")
    msg should include("Full error:")
  }

  it should "handle missing optional fields" in {
    val minimalInfo = OaiRequestInfo("ListRecords", None, None, None, 0)
    val msg = OaiHarvestException.formatForEmail(
      minimalInfo,
      sampleUrl,
      "page_parse",
      None,
      None,
      sampleCause
    )
    msg should include("OAI debug context:")
    msg should not include "Set:"
    msg should not include "Resumption token:"
    msg should not include "Cursor:"
    msg should include("Full error:")
  }

  "OaiHarvestException.buildEmailBody" should "produce complete email with hub and error details" in {
    val info = OaiRequestInfo(
      "ListRecords",
      Some("oai_qdc_imdpla"),
      Some("PFW_cc_fw_elect"),
      None,
      1771279252935L
    )
    val oaiErr = BadArgument(info)
    val rawCause = new RuntimeException(s"OAI Error: $oaiErr")
    val ex = new OaiHarvestException(
      info,
      "https://example.com/oai",
      "oai_error",
      oaiError = Some(oaiErr),
      cause = rawCause
    )
    val body = OaiHarvestException.buildEmailBody("indiana", ex)
    body should include("DPLA OAI Harvest Failure")
    body should include("Hub: indiana")
    body should include("Error:")
    body should include("OAI harvest error at stage oai_error")
    body should include("Set: PFW_cc_fw_elect")
    body should include("OAI Error: BadArgument")
    body should include("MetadataPrefix: oai_qdc_imdpla")
    body should include("contact tech@dp.la")
  }

  it should "include human-readable OAI error labels" in {
    val info =
      OaiRequestInfo("ListRecords", Some("oai_dc"), Some("test_set"), None, 0)
    val oaiErr = CannotDisseminateFormat(info)
    val rawCause = new RuntimeException(s"OAI Error: $oaiErr")
    val ex = new OaiHarvestException(
      info,
      "https://example.com/oai",
      "oai_error",
      oaiError = Some(oaiErr),
      cause = rawCause
    )
    val body = OaiHarvestException.buildEmailBody("test_hub", ex)
    body should include("OAI Error: CannotDisseminateFormat")
    body should include("MetadataPrefix: oai_dc")
    body should include("OAI Set: test_set")
    body should include("OriginalMessage:")
  }

  "OaiHarvestException.buildGenericEmailBody" should "produce email for non-OAI failures" in {
    val cause = new RuntimeException("Connection timed out")
    val body = OaiHarvestException.buildGenericEmailBody("maryland", cause)
    body should include("DPLA Harvest Failure")
    body should include("Hub: maryland")
    body should include("RuntimeException: Connection timed out")
    body should include("contact tech@dp.la")
  }
}
