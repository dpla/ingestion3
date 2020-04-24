package dpla.ingestion3.mappers.providers


import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class WiMappingTest extends FlatSpec with BeforeAndAfter {

  val shortName = "wi"
  val xmlString: String = new FlatFileIO().readFileAsString("/wi.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new WiMapping

  it should "extract the correct original id" in {
    val expected = Some("urn:ohiodplahub.library.ohio.gov:bgsu_12:oai:digitalgallery.bgsu.edu:14058")
    assert(extractor.originalId(xml) == expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/7e54c54e8f3b49009dc91d2568e021b5"))
    assert(extractor.dplaUri(xml) === expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq(uriOnlyWebResource(URI("https://digitalgallery.bgsu.edu/collections/item/14058")))
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract Seq() and not Seq('')_if only Rights exists" in {
    val expected = Seq()
    assert(extractor.rights(xml) === expected)
  }
}
