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

  it should "extract the correct provider id" in {
    val expected = "urn:ohiodplahub.library.ohio.gov:bgsu_12:oai:digitalgallery.bgsu.edu:14058"
    assert(extractor.getProviderId(xml) == expected)
  }

  it should "use the provider ID for the original ID" in
    assert(extractor.getProviderId(xml) == extractor.originalId(xml).get)

  it should "extract the correct isShownAt" in {
    val expected = Seq(uriOnlyWebResource(URI("https://digitalgallery.bgsu.edu/collections/item/14058")))
    assert(extractor.isShownAt(xml) === expected)
  }
}
