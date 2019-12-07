package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class GpoMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "gpo"
  val xmlString: String = new FlatFileIO().readFileAsString("/gpo.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new GpoMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName)

  it should "extract the correct original identifier " in {
    val expected = Some("oai:catalog.gpo.gov:GPO01-000003356")
    assert(extractor.originalId(xml) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/3bb32d23d7fef51d07bf4bb75b23ae31"))
    assert(extractor.dplaUri(xml) === expected)
  }

//  it should "extract the correct contributor" in {
//    val expected = Seq("Everhart, Benjamin Matlock, 1818-1904.", "Academy of Natural Sciences of Philadelphia.")
//      .map(nameOnlyAgent)
//    assert(extractor.contributor(xml) == expected)
//  }

  it should "extract the correct creator" in {
    val expected = Seq("Thackray, Richard I.").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct date" in {
    val expected = Seq("1975.").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in {
    val expected = Seq(
      "Cover title.",
      "\"FAA-AM-75-8.\"",
      "Prepared by Civil Aeromedical Institute, Oklahoma City, under task AM-C-75-PSY-48.",
      "[pbk. $3.00, mf $0.95, 7 cds]",
      "Also available via Internet from the FAA web site. Address as of 5/15/07: http://www.faa.gov/library/reports/medical/oamtechreports/1970s/media/AM75-08.pdf; current access is available via PURL.",
      "Includes bibliographical references (p. 9)."
      )
    assert(extractor.description(xml) == expected)
  }

  it should "extract the correct extent" in {
    val expected = Seq("9 p. ")
    assert(extractor.extent(xml) == expected)
  }

  it should "extract the correct format" in {
    val expected = Seq("Language material")
    assert(extractor.format(xml) == expected)
  }

  it should "extract the correct identifiers" in {
    val expected = Seq(
      "LC call number: RC1075",
      "gp^76003356",
      "(OCoLC)2281659",
      "0431-E-04",
      "0431-E-04 (online)",
      "TD 4.210:75-8")
    assert(extractor.identifier(xml) == expected)
  }
}
