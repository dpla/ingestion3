package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import org.scalatest.{BeforeAndAfter, FlatSpec}

class PaMappingTest extends FlatSpec with BeforeAndAfter {

  val extractor = new PaMapping

  it should "extract the correct original ID" in {
    val xml = <record><header><identifier>foo</identifier></header></record>
    val expected = Some("foo")
    assert(extractor.originalId(Document(xml)) == expected)
  }
}
