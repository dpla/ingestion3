package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model.URI
import org.scalatest.{BeforeAndAfter, FlatSpec}

class PaMappingTest extends FlatSpec with BeforeAndAfter {

  val extractor = new PaMapping

  it should "extract the correct original ID" in {
    val xml = <record><header><identifier>foo</identifier></header></record>
    val expected = Some("foo")
    assert(extractor.originalId(Document(xml)) == expected)
  }

  it should "create the correct DPLA URI" in {
    val xml = <record><header><identifier>foo</identifier></header></record>
    val expected = Some(URI("http://dp.la/api/items/acbd18db4cc2f85cedef654fccc4a4d8"))
    assert(extractor.dplaUri(Document(xml)) === expected)
  }
}
