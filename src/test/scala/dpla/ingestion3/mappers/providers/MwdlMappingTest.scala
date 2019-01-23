package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class MwdlMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "mwdl"
  val xmlString: String = new FlatFileIO().readFileAsString("/mwdl.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new MwdlMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName())

  it should "extract the correct original identifier " in
    assert(extractor.originalId(xml) === Some("digcoll_slc_27works_598"))

  it should "extract the correct collection titles" in {
    val expected = Seq("Salt Lake Community College Scholarly and Creative Works")
      .map(nameOnlyCollection)
    assert(extractor.collection(xml) === expected)
  }

  it should "extract the correct contributors" in {
    val expected = Seq("Salt Lake Community College (Creator)").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }
  it should "extract the correct dates" in {
    val expected = Seq("2012").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }
  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("Fall 2012 issue of Folio."))

  it should "extract the correct place values" in {
    val expected = Seq("Place").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }
  it should "extract the correct rights value" in {
    assert(extractor.rights(xml) === Seq("https://creativecommons.org/licenses/by-nc/4.0/"))
  }
  it should "extract the correct subjects" in {
    val expected = Seq("Art", "Literature", "Poetry", "writing").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }
  it should "extract the correct titles" in {
    val expected = Seq("Folio: To Do Something With the Sky")
    assert(extractor.title(xml) === expected)
  }
  it should "extract the correct types" in {
    val expected = Seq("text")
    assert(extractor.`type`(xml) === expected)
  }
  it should "extract the correct dataProvider" in {
    val expected = Seq(nameOnlyAgent("Salt Lake Community College Libraries"))
    assert(extractor.dataProvider(xml) === expected)
  }
  it should "extract the correct isShownAt" in {
    val expected = Seq(uriOnlyWebResource(URI("http://utah-primoprod.hosted.exlibrisgroup.com/primo_library/libweb/action/dlDisplay.do?vid=MWDL&afterPDS=true&docId=digcoll_slc_27works_598")))
    assert(extractor.isShownAt(xml) === expected)
  }
  it should "extract the correct preview" in {
    val expected = Seq("https://libarchive.slcc.edu/islandora/object/works_598/datastream/TN/",
      "https://libarchive.slcc.edu/islandora/object/works_598/datastream/TN/").map(stringOnlyWebResource)
    assert(extractor.preview(xml) === expected)
  }
  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/5c31abd09b535552592bf97cbed6557a"))
    assert(extractor.dplaUri(xml) === expected)
  }
}

