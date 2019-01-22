package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import org.scalatest.{BeforeAndAfter, FlatSpec}

class PaMappingTest extends FlatSpec with BeforeAndAfter {

  val extractor = new PaMapping

  it should "extract the correct provider ID" in {
    val xml = <record><header><identifier>foo</identifier></header></record>
    val expected = "foo"
    assert(extractor.getProviderId(Document(xml)) == expected)
  }

  it should "use the provider ID for the original ID" in {
    val xml = <record><header><identifier>foo</identifier></header></record>
    assert(extractor.getProviderId(Document(xml)) == extractor.originalId(Document(xml)))
  }
}
