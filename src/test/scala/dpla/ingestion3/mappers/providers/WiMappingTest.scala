package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class WiMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "wi"
  val xmlString: String = new FlatFileIO().readFileAsString("/wi.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new WiMapping


  it should "extract the correct isShownAt" in {
    val expected = uriOnlyWebResource(new URI("https://digitalgallery.bgsu.edu/collections/item/14058"))
    assert(extractor.isShownAt(xml)(msgCollector) === expected)
  }

  it should "record an error message if missing isShownAt" in {
    val xml =
      <record>
        <header>
          <identifier>urn:ohiodplahub.library.ohio.gov:bgsu_12:oai:digitalgallery.bgsu.edu:14058</identifier>
          <datestamp>2018-02-20</datestamp>
          <setSpec>bgsu_12</setSpec>
        </header>
        <metadata>
        </metadata>
      </record>

    extractor.isShownAt(Document(xml))
    assert(msgCollector
      .getAll()
      .contains(
        extractor.missingRequiredError("urn:ohiodplahub.library.ohio.gov:bgsu_12:oai:digitalgallery.bgsu.edu:14058", "isShownAt")
      ))
  }
}