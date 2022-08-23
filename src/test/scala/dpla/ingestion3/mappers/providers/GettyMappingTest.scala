package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class GettyMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "getty"
  val string: String = new FlatFileIO().readFileAsString("/getty.json")
  val xml: Document[JValue] = Document(parse(string))
  val extractor = new GettyMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName())

  it should "extract the correct original identifier " in
    assert(extractor.originalId(xml) === Some("GETTY_ROSETTAIE1318448"))

  it should "extract the correct collection titles" in {
    val expected = Seq("Julius Shulman photography archive, 1936-1997. Series IV. Job numbers, 1934-2009$$QJulius Shulman photography archive 1936 1997 Series IV Job numbers 1934 2009")
      .map(nameOnlyCollection)
    assert(extractor.collection(xml) === expected)
  }

  it should "extract the correct contributors" in {
    val expected = Seq("Contributor").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }
  it should "extract the correct dates" in {
    val expected = Seq("1954").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }
  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("Julius Shulman, 1910-2009, was an active architectural photographer from 1936 until 1986. Representing his career, the archive documents the modern movement in architecture spanning several decades and serves as a historical record of the Southern California landscape. Shulman's prolific career has helped to promote and broaden the knowledge of modern architecture by the thoughtful manner which he conveyed architectural design.",
      "Series IV contains the bulk of Shulman's archive. Represented here are more than 5,000 jobs that Shulman did not separate into smaller series based on architect or project. Most of the following descriptions include the names of artists, landscape designers or interior designers, but not builders, contractors or other clients.",
      "53 items"))

  it should "extract the correct place values" in {
    val expected = Seq("Los Angeles, Calif.").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }
  it should "extract the correct publishers" in {
    val expected = Seq("Publisher").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }
  it should "extract the correct rights value" in {
    assert(extractor.rights(xml) === Seq("For more information, see the <a href='http://hdl.handle.net/10020/repro_perm'>Library Reproductions & Permissions page.</a>"))
  }
  it should "extract the correct subjects" in {
    val expected = Seq("Architecture, Modern--20th century").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }
  it should "extract the correct titles" in {
    val expected = Seq("Job 1791: Josef Van der Kar, Wohlstetter House (Los Angeles, Calif.), 1954")
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
    val expected = Seq(stringOnlyWebResource("https://rosettaapp.getty.edu/delivery/DeliveryManagerServlet?dps_pid=IE1318448"))
    assert(extractor.isShownAt(xml) === expected)
  }
  it should "extract the correct preview" in {
    val expected = Seq(
      "https://rosettaapp.getty.edu/delivery/DeliveryManagerServlet?dps_pid=IE1318448&dps_func=thumbnail",
      "https://rosettaapp.getty.edu/delivery/DeliveryManagerServlet?dps_pid=IE1318448&dps_func=thumbnail")
      .map(stringOnlyWebResource)
    assert(extractor.preview(xml) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/5d9b9564a6694799ef391438be7d8a2b"))
    assert(extractor.dplaUri(xml) === expected)
  }
}

