package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}
import dpla.ingestion3.model._

import scala.xml.{NodeSeq, XML}

class OhioMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "ohio"
  val xmlString: String = new FlatFileIO().readFileAsString("/ohio.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new OhioMapping

  it should "not use the provider shortname in minting IDs "in
    assert(!extractor.useProviderName())

  it should "extract the correct provider identifier " in
    assert(extractor.getProviderId(xml) === "urn:ohiodplahub.library.ohio.gov:bgsu_12:oai:digitalgallery.bgsu.edu:14058")

  it should "throw an Exception if document does not contain a provider identifier" in {
    val xml = <record><metadata></metadata></record>
    assertThrows[Exception] {
      extractor.getProviderId(Document(xml))
    }
  }

  it should "extract the correct alternate titles " in {
    val expected = Seq("Alt title one", "Alt title two")
    assert(extractor.alternateTitle(xml) === expected)
  }

  it should "extract the correct collection titles" in {
    val expected = Seq("College of Musical Arts Programs", "A second collection")
      .map(nameOnlyCollection)
    assert(extractor.collection(xml) === expected)
  }

  it should "extract the correct contributors" in {
    val expected = Seq("Kantorksi, Valrie", "Pope, Ann").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }
  it should "extract the correct contributors when the value contains a ';'" in {
    val xml: NodeSeq =
      <record><metadata>
        <dcterms:contributor>Scott; Ted</dcterms:contributor>
        <dcterms:contributor>John</dcterms:contributor>
      </metadata></record>

    val expected = Seq("Scott", "Ted", "John").map(nameOnlyAgent)
    assert(extractor.contributor(Document(xml)) == expected)
  }

  it should "extract the correct creators" in {
    val expected = Seq("Sam G.", "Merry B.").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }
  it should "extract the correct creators when the value contains a ';'" in {
    val xml: NodeSeq =
      <record><metadata>
        <dcterms:creator>Huey; Lewy</dcterms:creator>
        <dcterms:creator>Dewy</dcterms:creator>
      </metadata></record>

    val expected = Seq("Huey", "Lewy", "Dewy").map(nameOnlyAgent)
    assert(extractor.creator(Document(xml)) == expected)
  }

  it should "extract the correct dates and split around ;" in {
    val expected = Seq("1990-02-09", "2008", "2016").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("A description", "A second description"))

  it should "extract the correct extent" in {
    val expected = Seq("21.5 centimeters high, 14 centimeters wide")
    assert(extractor.extent(xml) === expected)
  }

  it should "extract the correct format" in {
    val xml: Document[NodeSeq] = Document(<record><metadata>
      <dcterms:format>photograph</dcterms:format>
    </metadata></record>)
    val expected = Seq("photograph")
    assert(extractor.format(xml) === expected)
  }
  it should "extract the correct format and remove values in that are stop words ('image')" in {
    val xml: Document[NodeSeq] = Document(<record><metadata>
      <dcterms:format>photograph</dcterms:format>
      <dcterms:format>1 x 2 x 3</dcterms:format>
      <dcterms:format>image</dcterms:format>
    </metadata></record>)
    val expected = Seq("photograph")
    assert(extractor.format(xml) === expected)
  }
  it should "extract nothing if the format value looks like an extent" in {
    val xml: Document[NodeSeq] = Document(<record><metadata>
      <dcterms:format>1 score (3 p.) 33 cm</dcterms:format>
    </metadata></record>)
    val expected = Seq()
    assert(extractor.format(xml) === expected)
  }
  it should "extract the correct format when splitting on ';'  and remove values in that are stop words" in {
    val xml: Document[NodeSeq] = Document(<record><metadata>
      <dcterms:format>photograph; image</dcterms:format>
      <dcterms:format>archival box</dcterms:format>
    </metadata></record>)
    val expected = Seq("photograph", "archival box")
    assert(extractor.format(xml) === expected)
  }
  it should "extract no format when values are in format blacklist" in {
    val xml: Document[NodeSeq] = Document(<record><metadata><dcterms:format>image/jpeg</dcterms:format></metadata></record>)
    val expected = Seq()
    assert(extractor.format(xml) === expected)
  }

  it should "extract the correct identifier" in {
    val expected = Seq("FA19900209")
    assert(extractor.identifier(xml) === expected)
  }

  it should "extract the correct language when the value contains ';'" in {
    val expected = Seq("english", "eng").map(nameOnlyConcept)
    assert(extractor.language(xml) === expected)
  }
  it should "extract the correct language" in {
    val xml: NodeSeq =
      <record><metadata>
        <dcterms:language>english</dcterms:language>
        <dcterms:language>eng</dcterms:language>
      </metadata></record>
    val expected = Seq("english", "eng").map(nameOnlyConcept)
    assert(extractor.language(Document(xml)) === expected)
  }

  it should "extract the correct place values and split around ;" in {
    val expected = Seq("Ohio--Terrace Park", "Cincinnati, OH", "Hamilton County", "Butler County").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct publishers" in {
    val expected = Seq("Hottee Trusty", "Big 'ol publisher").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract the correct relation" in {
    val expected = Seq("College of Musical Arts Programs; MUSIC 003; Music Library " +
      "and Bill Schurk Sound Archives; University Libraries; " +
      "Bowling Green State University", "Faculty Artist Concert").map(eitherStringOrUri)
    assert(extractor.relation(xml) === expected)
  }

  it should "extract no rights value if the prefix is not 'dc'" in
    assert(extractor.rights(xml) === Seq())

  it should "extract rights value if the prefix is 'dc'" in {
    val rightsXml: Document[NodeSeq] = Document(<record xmlns:dc="http://purl.org/dc/elements/1.1/"><metadata><dc:rights>rights statement</dc:rights></metadata></record>)
    val expected = Seq("rights statement")
    assert(extractor.rights(rightsXml) === expected)
  }

  it should "extract the correct rightsHolder" in {
    val expected = Seq("Eric Rights Holder").map(nameOnlyAgent)
    assert(extractor.rightsHolder(xml) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq("Recital programs").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("Valrie Kantorski & Ann Pope")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct types" in {
    val expected = Seq("Image","Text")
    assert(extractor.`type`(xml) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = new URI("http://dp.la/api/items/130137a53d59ee27d7dab4e0078e1220")
    assert(extractor.dplaUri(xml) === expected)
  }

  // dataProvider
  it should "extract the correct dataProvider" in {
    val expected = nameOnlyAgent("Bowling Green State University Libraries")
    assert(extractor.dataProvider(xml) === expected)
  }
  it should "throw an exception if no dataProvider" in {
    val xml = <record><metadata></metadata></record>
    assertThrows[Exception] {
      extractor.dataProvider(Document(xml))
    }
  }

  it should "extract the correct edmRights" in {
    val expected = Some(new URI("http://rightsstatements.org/page/NoC-US/1.0/"))
    assert(extractor.edmRights(xml) === expected)
  }
  it should "extract the correct isShownAt" in {
    val expected = uriOnlyWebResource(new URI("https://digitalgallery.bgsu.edu/collections/item/14058"))
    assert(extractor.isShownAt(xml) === expected)
  }
  it should "throw an Exception if no isShownAt" in {
    val xml = <record><metadata></metadata></record>
    assertThrows[Exception] {
      extractor.isShownAt(Document(xml))
    }
  }
  it should "extract the correct preview" in {
    val expected = Some(uriOnlyWebResource(new URI("https://digitalgallery.bgsu.edu/files/thumbnails/26e197915e9107914faa33ac166ead5a.jpg")))
    assert(extractor.preview(xml) === expected)
  }

  // Extent format helper
  it should "extract extent values from format" in {
    val xml: NodeSeq = <record><metadata>
        <format>1 x 2 x 3</format>
        <format>written</format>
      </metadata></record>

    val expected = Seq("1 x 2 x 3")
    assert(extractor.extentFromFormat(Document(xml)) === expected)
  }
}