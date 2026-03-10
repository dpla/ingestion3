package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.harvesters.oai.{OaiPage, OaiRequestInfo, OaiXmlParser}
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class OaiXmlParserTest extends AnyFlatSpec with Matchers {
  private val flatFileIO = new FlatFileIO()
  private val requestInfo = OaiRequestInfo("verb", None, None, None, System.currentTimeMillis())

  "OaiXmlParser.parsePageIntoXml" should "correctly parse a string into an XML node" in {
    val xmlString = "<root><child>value</child></root>"
    val xmlNode = OaiXmlParser.parsePageIntoXml(OaiPage(xmlString, requestInfo))
    (xmlNode \ "child").text shouldEqual "value"
  }

  "OaiXmlParser.parseXmlIntoRecords" should "correctly parse XML into a sequence of OaiRecord objects" in {
    val xmlString = flatFileIO.readFileAsString("/oai-page.xml")
    val xmlNode = OaiXmlParser.parsePageIntoXml(OaiPage(xmlString, requestInfo))
    val records =
      OaiXmlParser.parseXmlIntoRecords(xmlNode, removeDeleted = false, requestInfo)
    records should have size 10
    records.head.id shouldEqual "arn00007c00001"
    records(1).id shouldEqual "arn00007c00002"
  }

  "OaiXmlParser.parseXmlIntoSets" should "correctly parse XML into a sequence of OaiSet objects" in {
    val xmlString = flatFileIO.readFileAsString("/oai-sets.xml")
    val xmlNode = OaiXmlParser.parsePageIntoXml(OaiPage(xmlString, requestInfo))
    val sets = OaiXmlParser.parseXmlIntoSets(xmlNode, requestInfo)
    sets should have size 50
    sets.head.id shouldEqual "crimes"
    sets(1).id shouldEqual "scarlet"
  }

  "OaiXmlParser.getResumptionToken" should "correctly extract the resumption token from XML" in {
    val xmlString = "<root><resumptionToken>token123</resumptionToken></root>"
    val xmlNode = OaiXmlParser.parsePageIntoXml(OaiPage(xmlString, requestInfo))
    val token = OaiXmlParser.getResumptionToken(xmlNode)
    token shouldEqual Some("token123")
  }

  "OaiXmlParser.containsError" should "throw an exception when the XML contains an error" in {
    val xmlString = "<root><error>Error message</error></root>"
    val xmlNode = OaiXmlParser.parsePageIntoXml(OaiPage(xmlString, requestInfo))
    an[RuntimeException] should be thrownBy OaiXmlParser.containsError(xmlNode)
  }

  // --- Invalid XML tests ---

  "OaiXmlParser.parsePageIntoXml" should "throw on invalid XML (unclosed tag)" in {
    val badXml = "<OAI-PMH><ListRecords><record>unclosed"
    an[Exception] should be thrownBy OaiXmlParser.parsePageIntoXml(OaiPage(badXml, requestInfo))
  }

  it should "throw on invalid XML (bad entity)" in {
    val badXml = "<root><child>&badentity;</child></root>"
    an[Exception] should be thrownBy OaiXmlParser.parsePageIntoXml(OaiPage(badXml, requestInfo))
  }

  // --- ResumptionToken with attributes ---

  "OaiXmlParser.getResumptionTokenWithAttrs" should "extract token, cursor, and completeListSize" in {
    val xmlString = """<root><resumptionToken cursor="100" completeListSize="4147">AoEtoken123</resumptionToken></root>"""
    val xmlNode = OaiXmlParser.parsePageIntoXml(OaiPage(xmlString, requestInfo))
    val result = OaiXmlParser.getResumptionTokenWithAttrs(xmlNode)
    result shouldBe Some(("AoEtoken123", Some(100), Some(4147)))
  }

  it should "return None when no resumption token is present" in {
    val xmlString = "<root><child>value</child></root>"
    val xmlNode = OaiXmlParser.parsePageIntoXml(OaiPage(xmlString, requestInfo))
    OaiXmlParser.getResumptionTokenWithAttrs(xmlNode) shouldBe None
  }

  it should "handle token without attributes" in {
    val xmlString = "<root><resumptionToken>simpletoken</resumptionToken></root>"
    val xmlNode = OaiXmlParser.parsePageIntoXml(OaiPage(xmlString, requestInfo))
    val result = OaiXmlParser.getResumptionTokenWithAttrs(xmlNode)
    result shouldBe Some(("simpletoken", None, None))
  }

  it should "handle token with only cursor attribute" in {
    val xmlString = """<root><resumptionToken cursor="50">token50</resumptionToken></root>"""
    val xmlNode = OaiXmlParser.parsePageIntoXml(OaiPage(xmlString, requestInfo))
    val result = OaiXmlParser.getResumptionTokenWithAttrs(xmlNode)
    result shouldBe Some(("token50", Some(50), None))
  }

  // --- Identifier extraction ---

  "OaiXmlParser.extractIdentifiers" should "extract first and last identifiers from valid XML" in {
    val page = """<OAI-PMH><ListRecords>
      <record><header><identifier>oai:example:001</identifier></header></record>
      <record><header><identifier>oai:example:002</identifier></header></record>
      <record><header><identifier>oai:example:050</identifier></header></record>
    </ListRecords></OAI-PMH>"""
    val (first, last) = OaiXmlParser.extractIdentifiers(page)
    first shouldBe Some("oai:example:001")
    last shouldBe Some("oai:example:050")
  }

  it should "extract identifiers from malformed XML" in {
    val page = """<OAI-PMH><ListRecords>
      <record><header><identifier>oai:broken:001</identifier></header>
      <record><header><identifier>oai:broken:010</identifier></header>
      unclosed garbage &badentity;"""
    val (first, last) = OaiXmlParser.extractIdentifiers(page)
    first shouldBe Some("oai:broken:001")
    last shouldBe Some("oai:broken:010")
  }

  it should "return None for both when no identifiers present" in {
    val page = "<root>no identifiers here</root>"
    val (first, last) = OaiXmlParser.extractIdentifiers(page)
    first shouldBe None
    last shouldBe None
  }
}
