package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class TnMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "tn"
  val xmlString: String = new FlatFileIO().readFileAsString("/tn.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new TnMapping

  it should "extract the correct alternate title" in {
    val expected = Seq("alt title 1")
    assert(extractor.alternateTitle(xml) == expected)
  }

  it should "extract the correct collection titles" in {
    val expected = Seq("project title", "collection title")
      .map(nameOnlyCollection)
    assert(extractor.collection(xml) == expected)
  }

  it should "extract the correct contributors" in {
    val expected = Seq("Boston Cooking School (Boston, Mass.)")
      .map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("eng")
      .map(nameOnlyConcept)
    assert(extractor.language(xml) == expected)
  }
}
