package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}
import dpla.ingestion3.model._

import scala.xml.{NodeSeq, XML}

class VirginiaMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "virginia"
  val xmlString: String = new FlatFileIO().readFileAsString("/virginia.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new VirginiaMapping

  it should "not use the provider shortname in minting IDs "in
    assert(!extractor.useProviderName())

  it should "extract the correct provider identifier " in
    assert(extractor.getProviderId(xml) === "uva-lib:1002813")

  it should "throw an Exception if document does not contain a provider identifier" in {
    val xml = <mdRecord></mdRecord>
    assertThrows[Exception] {
      extractor.getProviderId(Document(xml))
    }
  }

  it should "extract the correct collection titles" in {
    val expected =
      Seq("University of Virginia Printing Services photograph file",
        "University of Virginia Visual History Collection")
      .map(nameOnlyCollection)
    assert(extractor.collection(xml) === expected)
  }

  it should "extract the correct creators" in {
    val expected =
      Seq("Goings, Henry, approximately 1810-",
        "Runkle, Benjamin Piatt, 1837-1916",
        "Catlin, Isaac S. (Isaac Swartwood), 1835-1916",
        "Robb, James M.")
        .map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct dates" in {
    val expected = Seq("1869").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("A description."))

  it should "extract the correct extent" in {
    val expected = Seq("iv, [5]-72 p. ; 22 cm. (8vo)")
    assert(extractor.extent(xml) === expected)
  }

//  it should "extract the correct format" in {
//    val xml: Document[NodeSeq] = Document(<record><metadata>
//      <dcterms:format>photograph</dcterms:format>
//    </metadata></record>)
//    val expected = Seq("photograph")
//    assert(extractor.format(xml) === expected)
//  }
//  it should "extract the correct format and remove values in that are stop words ('image')" in {
//    val xml: Document[NodeSeq] = Document(<record><metadata>
//      <dcterms:format>photograph</dcterms:format>
//      <dcterms:format>1 x 2 x 3</dcterms:format>
//      <dcterms:format>image</dcterms:format>
//    </metadata></record>)
//    val expected = Seq("photograph")
//    assert(extractor.format(xml) === expected)
//  }
//  it should "extract nothing if the format value looks like an extent" in {
//    val xml: Document[NodeSeq] = Document(<record><metadata>
//      <dcterms:format>1 score (3 p.) 33 cm</dcterms:format>
//    </metadata></record>)
//    val expected = Seq()
//    assert(extractor.format(xml) === expected)
//  }

//  it should "extract no format when values are in format blacklist" in {
//    val xml: Document[NodeSeq] = Document(<record><metadata><dcterms:format>image/jpeg</dcterms:format></metadata></record>)
//    val expected = Seq()
//    assert(extractor.format(xml) === expected)
//  }
//
  it should "extract the correct identifier" in {
    val expected = Seq("uva-lib:1002813")
    assert(extractor.identifier(xml) === expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("eng").map(nameOnlyConcept)
    assert(extractor.language(xml) === expected)
  }

  it should "extract the correct place" in {
    val expected = Seq("Canada", "Ontario", "Stratford").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct publishers" in {
    val expected = Seq("Printed by J.M. Robb, Herald Office").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract rights value'" in {
    val expected = Seq("http://rightsstatements.org/vocab/NoC-US/1.0/")
    assert(extractor.rights(xml) === expected)
  }

  it should "extract the correct subjects" in {
    val expected =
      Seq("Goings, Henry, approximately 1810-",
      "Slave narratives",
        "African Americans--Biography")
      .map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("Rambles of a runaway from Southern slavery")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct types" in {
    val expected = Seq("Text")
    assert(extractor.`type`(xml) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = new URI("http://dp.la/api/items/ad6472a5e0575718616b5fd54c599095")
    assert(extractor.dplaUri(xml) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = List(nameOnlyAgent("University of Virginia Library"))
    assert(extractor.dataProvider(xml) === expected)
  }

  it should "extract the correct isShownAt" in {
    val expected =
      List(uriOnlyWebResource(new URI("http://search.lib.virginia.edu/catalog/uva-lib:1002813")))
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract the correct preview" in {
    val expected =
      List(uriOnlyWebResource(new URI("https://iiif.lib.virginia.edu/iiif/uva-lib:857804/full/!300,300/0/default.jpg")))
    assert(extractor.preview(xml) === expected)
  }
}
