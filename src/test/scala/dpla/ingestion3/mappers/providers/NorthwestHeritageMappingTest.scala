package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter}
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

class NorthwestHeritageMappingTest extends AnyFlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "nwdh"
  val xmlString: String = new FlatFileIO().readFileAsString("/northwest-heritage.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new NorthwestHeritageMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName)

  it should "extract the correct original identifier " in
    assert(extractor.originalId(xml) === Some("oai:nwdh:densho:ddr.densho.org:ddr-densho-101-1"))

  it should "extract the correct types" in {
    val expected = Seq("Image")
    assert(extractor.`type`(xml) === expected)
  }

  it should "extract the correct preview " in {
    val expected = Seq("https://ddr.densho.org/media/ddr-densho-101/ddr-densho-101-1-mezzanine-96054e814c-a.jpg")
      .map(stringOnlyWebResource)
    assert(extractor.preview(xml) === expected)
  }

  it should "extract the correct isShownAt " in {
    val expected = Seq("http://ddr.densho.org/ddr-densho-101-1/").map(stringOnlyWebResource)
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract the correct edmRights " in {
    val expected = Seq(URI("http://rightsstatements.org/vocab/InC/1.0/"))
    assert(extractor.edmRights(xml) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("Densho").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) === expected)
  }

  it should "extract the correct title" in {
    val expected = Seq("Group in front of the Japanese American Courier offices")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct subject" in {
    val expected = Seq("Industry and employment -- Journalism").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct place" in {
    val expected = Seq("Seattle, Washington (State), United States").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("Lenggenhager, Werner W., 1899-1988").map(nameOnlyAgent)
    assert(extractor.creator(xml) === expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("eng").map(nameOnlyConcept)
    assert(extractor.language(xml) === expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("(L to R): Yone Bartholomew, Clarence Arai, unidentified, Misao Sakamoto holding daughter Marie, Jimmie Sakamoto, Slocum Nishimura (Tokie Slocum) in front of the offices of the Japanese American Courier. Names: Bartholomew, Yone; Arai, Clarence; Sakamoto, Misao; Sakamoto, Marie; Sakamoto, Jimmie; Nishimura, Slocum")
    assert(extractor.description(xml) === expected)
  }

  it should "extract the correct date" in {
    val expected = Seq("1891?").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct collection title" in {
    val expected = Seq("Seattle Historical Photograph Collection").map(nameOnlyCollection)
    assert(extractor.collection(xml) === expected)
  }

  it should "extract the correct contributor" in {
    val expected = Seq("Thiry, Paul, 1904-1993").map(nameOnlyAgent)
    assert(extractor.contributor(xml) === expected)
  }

  it should "extract the correct identifier" in {
    val expected = Seq("ddr-densho-101-1")
    assert(extractor.identifier(xml) === expected)
  }

  it should "extract the correct formats" in {
    val expected = Seq("photographs")
    assert(extractor.format(xml) === expected)
  }

  it should "extract the correct extent" in {
    val expected = Seq("3.5 x 7 in.")
    assert(extractor.extent(xml) === expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("Watkins, Carleton E., 1829-1916").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract the correct iiifManifest" in {
    val expected = Seq("http://iiif.manifest/").map(URI)
    assert(extractor.iiifManifest(xml) === expected)
  }

  it should "extract the correct media master" in {
    val expected = Seq("http://media.master/").map(stringOnlyWebResource)
    assert(extractor.mediaMaster(xml) === expected)
  }

  it should "apply a nwdh tag" in {
    val expected = Seq(URI("nwdh"))
    assert(extractor.tags(xml) === expected)
  }

  it should "extract the correct freetext rights" in {
    val expected = Seq("blah blah blah")
    assert(extractor.rights(xml) === expected)
  }
}

