package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class NYPLMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "nypl"
  val xmlString: String = new FlatFileIO().readFileAsString("/nypl.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new NYPLMapping

  it should "extract the correct title " in {
    val expected = Seq("Jedediah Buxton  [National Calculator, 1705-1780]")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct alt titles" in {
    val expected = Seq("Alternate Title")
    assert(extractor.alternateTitle(xml) === expected)
  }

  it should "extract the correct identifiers" in {
    val expected = Seq("URN id")
    assert(extractor.identifier(xml) === expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("A valid note", "hello world.")
    assert(extractor.description(xml) === expected)
  }

  it should "extract the correct isShownAt value" in {
    val expected = Seq(stringOnlyWebResource("https://digitalcollections.nypl.org/items/4d0e0bc0-c540-012f-1857-58d385a7bc34"))
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq("temporal subject", "Public figures", "Subject title", "Subject name").map(nameOnlyConcept)
    assert(extractor.subject(xml) forall(expected contains))
  }

  it should "extract the correct temporal values" in {
    val expected = Seq("temporal subject").map(stringOnlyTimeSpan)
    assert(extractor.temporal(xml) === expected)
  }

  it should "extract the correct type" in {
    val expected = Seq("still image")
    assert(extractor.`type`(xml) === expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("Kay, John (1742-1826)").map(nameOnlyAgent)
    assert(extractor.creator(xml) === expected)
  }

  it should "extract the correct contributor" in {
    val expected = Seq("Contributor").map(nameOnlyAgent)
    assert(extractor.contributor(xml) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("The Miriam and Ira D. Wallach Division of Art, Prints and Photographs: Print Collection. The New York Public Library").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) === expected)
  }
}
