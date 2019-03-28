package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class IllinoisMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "il"
  val xmlString: String = new FlatFileIO().readFileAsString("/il.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new IllinoisMapping

  // use provider prefix for pre-hash ID
  it should "use the provider shortname in minting IDs "in
    assert(extractor.useProviderName())

  // original ID
  it should "extract the correct original ID" in {
    val expected = Some("http://collections.carli.illinois.edu/cdm/ref/collection/uic_pic/id/5601")
    assert(extractor.originalId(xml) == expected)
  }

  // alternate title
  it should "extract the correct alternate titles " in {
    val expected = List("Alt title")
    assert(extractor.alternateTitle(xml) === expected)
  }

  // contributor
  it should "extract the correct contributors" in {
    val expected = Seq("Contributor").map(nameOnlyAgent)
    assert(extractor.contributor(xml) === expected)
  }

  // creators
  it should "extract the correct creators" in {
    val expected = Seq("Copelin Commercial Photographers").map(nameOnlyAgent)
    assert(extractor.creator(xml) === expected)
  }

  // date
  it should "extract the correct dates" in {
    val expected = Seq("1997").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  // description
  it should "extract the correct description" in {
    val expected = Seq("Bridges, viaducts and underpasses included: Michigan Ave. Viaduct 9 (1997)")
    assert(extractor.description(xml) === expected)
  }

  // format
  it should "extract the correct format values" in {
    val expected = Seq("Photographs", "photographic prints")
    assert(extractor.format(xml) === expected)
  }

  // language
  it should "extract the correct languages " in {
    val expected = Seq("English").map(nameOnlyConcept)
    assert(extractor.language(xml) === expected)
  }

  // place
  it should "extract the correct places" in {
    val expected = Seq("Chicago (Ill.)").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }

  // rights
  it should "extract the correct rights" in {
    val expected = Seq("free text rights")
    assert(extractor.rights(xml) === expected)
  }

  // subject
  it should "extract the correct subject " in {
    val expected = Seq("subject").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  // temporal
  it should "extract the correct temporal " in {
    val expected = Seq("temporal").map(stringOnlyTimeSpan)
    assert(extractor.temporal(xml) === expected)
  }

  // title
  it should "extract the correct titles" in {
    val expected = Seq("Bridges, viaducts, and underpasses: Michigan Ave. Viaduct 9, Image 10")
    assert(extractor.title(xml) === expected)
  }

  // type
  it should "extract the correct type values" in {
    val expected = Seq("Image")
    assert(extractor.`type`(xml) === expected)
  }

  // dataProvider
  it should "extract the correct dataProvider " in {
    val expected = Seq("University of Illinois at Chicago").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) === expected)
  }

  // isShownAt
  it should "extract the correct isShownAt" in {
    val expected = List(stringOnlyWebResource("http://collections.carli.illinois.edu/cdm/ref/collection/uic_pic/id/5601"))
    assert(extractor.isShownAt(xml) === expected)
  }

  // preview
  it should "extract the correct preview" in {
    val expected = List(stringOnlyWebResource("http://collections.carli.illinois.edu/utils/getthumbnail/collection/uic_pic/id/5601"))
    assert(extractor.preview(xml) === expected)
  }
}