package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class OrbisCascadeMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "orbis-cascade"
  val xmlString: String = new FlatFileIO().readFileAsString("/orbis-cascade.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new OrbisCascadeMapping

  it should "use the provider shortname in minting IDs "in
    assert(extractor.useProviderName())

  it should "extract the correct original identifier " in
    assert(extractor.originalId(xml) === Some("oai:digital.lib.uidaho.edu:psychiana/0"))

  it should "extract the correct creators" in {
    val expected = Seq("Robinson, Frank B.").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct dates" in {
    val expected = Seq("1946").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("A description"))

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
    val expected = Seq("MG101_b5_f1_001", "http://digital.lib.uidaho.edu/cdm/ref/collection/psychiana/id/0")
    assert(extractor.identifier(xml) === expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("eng").map(nameOnlyConcept)
    assert(extractor.language(xml) === expected)
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
    val expected = Seq("God-Power; Psychiana Criticism; Psychiana Origins; God on Earth").map(nameOnlyConcept)
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
    val expected = Some(URI("http://dp.la/api/items/130137a53d59ee27d7dab4e0078e1220"))
    assert(extractor.dplaUri(xml) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = List(nameOnlyAgent("Bowling Green State University Libraries"))
    assert(extractor.dataProvider(xml) === expected)
  }

  it should "extract the correct edmRights" in {
    val expected = List(new URI("http://rightsstatements.org/page/NoC-US/1.0/"))
    assert(extractor.edmRights(xml) === expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = List(uriOnlyWebResource(new URI("https://digitalgallery.bgsu.edu/collections/item/14058")))
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract the correct preview" in {
    val expected = List(uriOnlyWebResource(new URI("https://digitalgallery.bgsu.edu/files/thumbnails/26e197915e9107914faa33ac166ead5a.jpg")))
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