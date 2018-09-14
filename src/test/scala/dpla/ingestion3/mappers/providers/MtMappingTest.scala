package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class MtMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "mt"
  val xmlString: String = new FlatFileIO().readFileAsString("/mt.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new MtMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName())
  it should "extract the correct provider identifier " in
    assert(extractor.getProviderId(xml) === "oai:the.european.library.UMr8:oai:scholarworks.umt.edu:goedicke-1008")
  it should "throw an Exception if document does not contain a provider identifier" in {
    val xml = <record><metadata></metadata></record>
    assertThrows[Exception] {
      extractor.getProviderId(Document(xml))
    }
  }
  it should "extract the correct collection titles" in {
    val expected = Seq("Collection Title")
      .map(nameOnlyCollection)
    assert(extractor.collection(xml) === expected)
  }
  it should "extract the correct creators" in {
    val expected = Seq("Goedicke, Patricia").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }
  it should "extract the correct dates" in {
    val expected = Seq("2001-10-07T07:00:00Z").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }
  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("Description"))

  it should "extract the correct extent" in
    assert(extractor.extent(xml) == Seq())

  it should "extract the correct format" in {
    val expected = Seq("Format")
    assert(extractor.format(xml) === expected)
  }
  it should "extract the correct format and remove values in that are stop words ('image')" in {
    val expected = Seq("Format")
    assert(extractor.format(xml) === expected)
  }
  it should "extract the correct place values" in {
    val expected = Seq("Geographic").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }
  it should "extract the correct publishers" in {
    val expected = Seq("Publisher").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }
  it should "extract the correct rights value" in {
    assert(extractor.rights(xml) === Seq("Copyright to this collection is held by the Maureen and Mike Mansfield Library, University of Montana-Missoula. For further information please contact Archives and Special Collections at the University of Montana, Mansfield Library: http://www.lib.umt.edu/asc"))
  }
  it should "extract the correct subjects" in {
    val expected = Seq(
      "Patricia Goedicke", "American literature--Montana",
      "American literature--20th century", "documents").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }
  it should "extract the correct titles" in {
    val expected = Seq("Reading notes for 2nd Wind 2001")
    assert(extractor.title(xml) === expected)
  }
  it should "extract the correct types" in {
    val expected = Seq("text")
    assert(extractor.`type`(xml) === expected)
  }
  it should "extract the correct dataProvider" in {
    val expected = nameOnlyAgent("University of Montana--Missoula. Mansfield Library")
    assert(extractor.dataProvider(xml) === expected)
  }
  it should "throw an exception if no dataProvider" in {
    val xml = <record><metadata></metadata></record>
    assertThrows[Exception] {
      extractor.dataProvider(Document(xml))
    }
  }
  it should "extract the correct isShownAt" in {
    val expected = uriOnlyWebResource(new URI("https://scholarworks.umt.edu/goedicke/9"))
    assert(extractor.isShownAt(xml) === expected)
  }
  it should "throw an Exception if no isShownAt" in {
    val xml = <record><metadata></metadata></record>
    assertThrows[Exception] {
      extractor.isShownAt(Document(xml))
    }
  }
  it should "extract the correct preview" in {
    val expected = Some(uriOnlyWebResource(new URI("https://scholarworks.umt.edu/goedicke/1008/thumbnail.jpg")))
    assert(extractor.preview(xml) === expected)
  }
}