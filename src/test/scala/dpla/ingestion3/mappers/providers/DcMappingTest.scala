package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class DcMappingTest extends FlatSpec with BeforeAndAfter {

  val shortName = "dc"
  val xmlString: String = new FlatFileIO().readFileAsString("/ohio.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new DcMapping

  it should "not use the provider shortname in minting IDs " in
    assert(!extractor.useProviderName())
  it should "extract the correct provider identifier " in
    assert(extractor.getProviderId(xml) === "urn:ohiodplahub.library.ohio.gov:bgsu_12:oai:digitalgallery.bgsu.edu:14058")
  it should "throw an Exception if document does not contain a provider identifier" in {
    val xml = <record><metadata></metadata></record>
    assertThrows[Exception] {
      extractor.getProviderId(Document(xml))
    }
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
  it should "extract the correct creators" in {
    val expected = Seq("Sam G.", "Merry B.").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }
  it should "extract the correct dates" in {
    val expected = Seq("1990-02-09", "2008; 2016").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }
  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("A description", "A second description"))

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
      <dcterms:format>image</dcterms:format>
    </metadata></record>)
    val expected = Seq("photograph")
    assert(extractor.format(xml) === expected)
  }
  it should "extract no format when values are in format blacklist" in {
    val xml: Document[NodeSeq] = Document(<record><metadata><dcterms:format>image/jpeg</dcterms:format></metadata></record>)
    assert(extractor.format(xml) === Seq())
  }
  it should "extract the correct identifier" in {
    assert(extractor.identifier(xml) === Seq("FA19900209"))
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
  it should "extract the correct place values" in {
    val xml: NodeSeq =
      <record><metadata>
        <place>Ohio--Terrace Park</place>
        <place>Cincinnati, OH</place>
      </metadata></record>
    val expected = Seq("Ohio--Terrace Park", "Cincinnati, OH").map(nameOnlyPlace)
    assert(extractor.place(Document(xml)) === expected)
  }
  it should "extract the correct publishers" in {
    val expected = Seq("Hottee Trusty", "Big 'ol publisher").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }
  it should "extract no rights value if the prefix is not 'dc'" in {
    assert(extractor.rights(xml) === Seq())
  }
  it should "extract rights value if the property prefix is 'dc'" in {
    val rightsXml: Document[NodeSeq] = Document(
    <record xmlns:dc="http://purl.org/dc/elements/1.1/">
      <metadata>
        <dc:rights>rights statement</dc:rights>
      </metadata>
    </record>)
    assert(extractor.rights(rightsXml) === Seq("rights statement"))
  }
  it should "extract the correct edmRights " in {
    val xml: Document[NodeSeq] = Document(
    <record xmlns:dc="http://purl.org/dc/elements/1.1/">
      <metadata>
        <dc:rights>Eric Rights Holder</dc:rights>
        <edm:rights>http://rights.holder.edu</edm:rights>
      </metadata>
    </record>)
    val expected = Option(new URI("http://rights.holder.edu"))
    assert(extractor.edmRights(xml) === expected)
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
    val expected = Seq("Image", "Text", "image/jpeg", "image", "photograph")
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
}