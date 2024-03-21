package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.BeforeAndAfter
import dpla.ingestion3.model._
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

class VtMappingTest extends AnyFlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "vt"
  val xmlString: String = new FlatFileIO().readFileAsString("/vt.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new VtMapping

  it should "not use the provider shortname in minting IDs "in
    assert(!extractor.useProviderName)

  it should "extract the correct original identifier " in
    assert(extractor.originalId(xml) === Some("http://archive.org/details/hhfbc-c7"))

  it should "create the correct DPLA URI" in {
    val expected = Some(new URI("http://dp.la/api/items/9df2bbb82c4434954a12454414ca4174"))
    assert(extractor.dplaUri(xml) === expected)
  }

  it should "extract the correct contributors" in {
    val expected = Seq("Wilson, Douglas B.").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  it should "extract the correct creators" in {
    val expected = Seq("Flanders, Helen Hartness, 1890-1972").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct dates" in {
    val expected = Seq("1948").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("Box 1, Cylinder 7"))

  it should "extract the correct format" in {
    val expected = Seq("Spectrogram")
    assert(extractor.format(xml) === expected)
  }

  it should "extract the correct identifiers" in {
    val expected = Seq("http://archive.org/details/hhfbc-c7", "RcXII_19_1921")
    assert(extractor.identifier(xml) === expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("English").map(nameOnlyConcept)
    assert(extractor.language(xml) === expected)
  }

  it should "extract the correct place" in {
    val expected = Seq("Shrewsbury (Vt.)").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct publishers" in {
    val expected = Seq("C. W. Hughes & Co., Inc., Mechanicville, N. Y.").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract the correct relations" in {
    val expected = Seq("Original from the Rodney Homeister Peterson Photographs.").map(eitherStringOrUri)
    assert(extractor.relation(xml) ==  expected)
  }

  it should "extract the correct rights" in {
    val expected = Seq("Free text rights statement")
    assert(extractor.rights(xml) == expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq("Helen Hartness Flanders", "Ballads", "Burlington (Vt.)").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct types" in {
    val expected = Seq("Text")
    assert(extractor.`type`(xml) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("Cylinder 7 - Flanders Ballad Collection IRENE")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("Middlebury College").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) === expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq("http://archive.org/details/150SNV").map(stringOnlyWebResource)
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract the correct edmRights" in {
    val expected = Seq("http://rightsstatements.org/vocab/CNE/1.0/").map(URI)
    assert(extractor.edmRights(xml) === expected)
  }

  it should "extract the correct preview" in {
    val expected = Seq("http://archive.org/download/150SNV/__ia_thumb.jpg").map(stringOnlyWebResource)
    assert(extractor.preview(xml) === expected)
  }

  it should "extract the correct iiif manifest" in {
    val expected = Seq("https://iiif.archivelab.org/iiif/1808.rockwoodc.18390316/manifest.json").map(URI)
    assert(extractor.iiifManifest(xml) === expected)
  }
}
