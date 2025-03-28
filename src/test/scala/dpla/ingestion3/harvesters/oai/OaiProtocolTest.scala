package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.harvesters.oai.{OaiConfiguration, OaiPage, OaiProtocol, OaiRequestInfo}
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.flatspec.AnyFlatSpec

class OaiProtocolTest extends AnyFlatSpec {

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
}
