package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class GettyMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "getty"
  val xmlString: String = new FlatFileIO().readFileAsString("/getty.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new GettyMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName())

  it should "extract the correct original identifier " in
    assert(extractor.originalId(xml) === Some("GETTY_OCPFL608236"))

  it should "extract the correct collection titles" in {
    val expected = Seq("Foto Arte Minore / Max Hutzel (accession number 86.P.8)")
      .map(nameOnlyCollection)
    assert(extractor.collection(xml) === expected)
  }

  it should "extract the correct contributors" in {
    val expected = Seq("Contributor").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }
  it should "extract the correct dates" in {
    val expected = Seq("1960-1990").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }
  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("German-born photographer and scholar Max Hutzel (1911-1988) photographed in Italy from the early 1960s until his death. The result of this project, referred to by Hutzel as Foto Arte Minore, is thorough documentation of art historical development in Italy up to the 18th century, including objects of the Etruscans and the Romans, as well as early Medieval, Romanesque, Gothic, Renaissance and Baroque monuments. Images are organized by geographic region in Italy, then by province, city, site complex and monument.", "Medieval: Entire manuscript. Manuscripts. Post-medieval: Sculpture and ceramics from local workshops (2nd half 12th century; Sienese maiolica; 14th century weapons", "Image 150 of 365"))

  it should "extract the correct place values" in {
    val expected = Seq("Place", "Place2").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }
  it should "extract the correct publishers" in {
    val expected = Seq("Publisher").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }
  it should "extract the correct rights value" in {
    assert(extractor.rights(xml) === Seq("Digital images courtesy of the Getty's Open Content Program."))
  }
  it should "extract the correct subjects" in {
    val expected = Seq("Subject").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }
  it should "extract the correct titles" in {
    val expected = Seq("Tuscany--Siena--Montalcino--Museo Civico, Image 150")
    assert(extractor.title(xml) === expected)
  }
  it should "extract the correct types" in {
    val expected = Seq("Still image")
    assert(extractor.`type`(xml) === expected)
  }
  it should "extract the correct dataProvider" in {
    val expected = Seq(nameOnlyAgent("Getty Research Institute"))
    assert(extractor.dataProvider(xml) === expected)
  }
  it should "extract the correct isShownAt" in {
    val expected = Seq(uriOnlyWebResource(URI("http://primo.getty.edu/primo_library/libweb/action/dlDisplay.do?vid=GRI-OCP&afterPDS=true&institution=01GRI&docId=GETTY_OCPFL608236")))
    assert(extractor.isShownAt(xml) === expected)
  }
  it should "extract the correct preview" in {
    val expected = Seq(
      "http://rosettaapp.getty.edu:1801/delivery/DeliveryManagerServlet?dps_pid=FL608236",
      "http://rosettaapp.getty.edu:1801/delivery/DeliveryManagerServlet?dps_pid=FL608236")
      .map(stringOnlyWebResource)
    assert(extractor.preview(xml) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/d825ec923b90df0ad638cace2f8f9e83"))
    assert(extractor.dplaUri(xml) === expected)
  }
}

