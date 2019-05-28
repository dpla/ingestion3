package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class BhlMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "bhl"
  val xmlString: String = new FlatFileIO().readFileAsString("/bhl.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new BhlMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName)

  it should "extract the correct original identifier " in {
    val expected = Some("???")
    assert(extractor.originalId(xml) === expected)
  }

  it should "extract the correct collection" in {
    ???
  }

  it should "extract the correct contributor" in {
    val expected = Seq("???").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("???").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct date" in {
    val expected = Seq("???").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("???")
    assert(extractor.description(xml) == expected)
  }

  it should "extract the correct format" in {
    val expected = Seq("???")
    assert(extractor.format(xml) == expected)
  }

  it should "extract the correct genre" in {
    val expected = Seq("???")
    assert(extractor.genre(xml) == expected)
  }

  it should "extract the correct identifier" in {
    val expected = Seq("???")
    assert(extractor.identifier(xml) == expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("???").map(nameOnlyConcept)
    assert(extractor.language(xml) == expected)
  }

  it should "extract the correct place" in {
    val expected = Seq("???").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("???").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract the correct relation" in {
    val expected = "???" // LiteralOrUri
    assert(extractor.relation(xml) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq("???").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct temporal" in {
    val expected = Seq("???").map(stringOnlyTimeSpan)
    assert(extractor.temporal(xml) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("???")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct types" in {
    val expected = Seq("???")
    assert(extractor.`type`(xml) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq(nameOnlyAgent("???"))
    assert(extractor.dataProvider(xml) === expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq(uriOnlyWebResource(URI("???")))
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract the correct preview" in {
    val expected = Seq("???").map(stringOnlyWebResource)
    assert(extractor.preview(xml) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("???"))
    assert(extractor.dplaUri(xml) === expected)
  }
}