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
    val expected = Some("oai:biodiversitylibrary.org:item/7")
    assert(extractor.originalId(xml) === expected)
  }

  it should "extract the correct collection" in {
    val expected = Seq("Flore de Buitenzorg ;  pt. 5.").map(nameOnlyCollection)
    assert(extractor.collection(xml) == expected)
  }

//  it should "extract the correct contributor" in {
//    val expected = Seq("Königliche Botanische Gesellschaft").map(nameOnlyAgent)
//    assert(extractor.contributor(xml) == expected)
//  }

//  it should "extract the correct creator" in {
//    val expected = Seq("Fleischer, Max,").map(nameOnlyAgent)
//    assert(extractor.creator(xml) == expected)
//  }

  it should "extract the correct date" in {
    val expected = Seq("1900-1902").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("pt.5:v.1 (1900-1902)")
    assert(extractor.description(xml) == expected)
  }

  it should "extract the correct format" in {
    val expected = Seq("print")
    assert(extractor.format(xml) == expected)
  }

  it should "extract the correct genre" in {
    val expected = Seq("book").map(nameOnlyConcept)
    assert(extractor.genre(xml) == expected)
  }

  it should "extract the correct identifier" in {
    val expected = Seq("https://www.biodiversitylibrary.org/item/7", "2497085", "10.5962/bhl.title.44870")
    assert(extractor.identifier(xml) == expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("Dutch").map(nameOnlyConcept)
    assert(extractor.language(xml) == expected)
  }

  it should "extract the correct place" in {
    val expected = Seq("Bogor", "Indonesia").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("E.J. Brill,").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract the correct rights" in {
    val expected = Seq("Public domain.  The BHL considers that this work is no longer under copyright protection.")
    assert(extractor.rights(xml) === expected)
  }

  it should "extract the correct relation" in {
    val expected = Seq("Flore de Buitenzorg ;  pt. 5.").map(eitherStringOrUri)
    assert(extractor.relation(xml) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq("Musci").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct temporal" in {
    val expected = Seq("19th century").map(stringOnlyTimeSpan)
    assert(extractor.temporal(xml) === expected)
  }

//  it should "extract the correct titles" in {
//    val expected = Seq("Die Musci der Flora von Buitenzorg : zugleich Laubmoosflora von Java /")
//    assert(extractor.title(xml) === expected)
//  }

  it should "extract the correct types" in {
    val expected = Seq("text")
    assert(extractor.`type`(xml) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq(nameOnlyAgent("Missouri Botanical Garden, Peter H. Raven Library"))
    assert(extractor.dataProvider(xml) === expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq(uriOnlyWebResource(URI("https://www.biodiversitylibrary.org/item/7")))
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract the correct preview" in {
    val expected = Seq("https://www.biodiversitylibrary.org/pagethumb/600316").map(stringOnlyWebResource)
    assert(extractor.preview(xml) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/0968d91df9d69566b805f29b1275b974"))
    assert(extractor.dplaUri(xml) === expected)
  }
}