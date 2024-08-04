package dpla.ingestion3.harvesters.oai.refactor

import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.flatspec.AnyFlatSpec

class OaiProtocolTest extends AnyFlatSpec {

  val oaiProtocol = new OaiProtocol(
    OaiConfiguration(Map("verb" -> "ListRecords"))
  )
  val flatFileIO = new FlatFileIO()

  "An OaiProtocol" should "parse a records page into records" in {
    val oaiXmlString = flatFileIO.readFileAsString("/oai-page.xml")
    val result = oaiProtocol.parsePageIntoRecords(
      OaiPage(oaiXmlString),
      removeDeleted = false
    )
    assert(Seq(result).nonEmpty)
  }

  it should "parse a set page into sets" in {
    val oaiXmlString = flatFileIO.readFileAsString("/oai-sets.xml")
    val result = oaiProtocol.parsePageIntoSets(OaiPage(oaiXmlString))
    assert(Seq(result).nonEmpty)
  }
}
