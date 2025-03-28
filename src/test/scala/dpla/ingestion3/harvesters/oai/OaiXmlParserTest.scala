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
}
