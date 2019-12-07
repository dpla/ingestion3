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
//    val expected = Seq("")
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

//  it should "extract the correct language" in {
//    val expected = Seq("").map(nameOnlyConcept)
//    assert(extractor.language(xml) == expected)
//  }

  it should "extract the correct place" in {
    val expected = Seq("United States").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("Washington, D.C. : U.S. Dept. of Transportation, Federal Aviation Administration, Office of Aviation Medicine,").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

//  it should "extract the correct relation" in {
//    val expected =
//      Seq("").map(eitherStringOrUri)
//    assert(extractor.relation(xml) === expected)
//  }

//  it should "extract the correct rights" in {
//    val expected = Seq("")
//    assert(extractor.rights(xml) === expected)
//  }

  it should "provide default rights statement" in {
    val xml =
      <record>
        <metadata>
          <marc:record>
          </marc:record>
        </metadata>
      </record>

    val default = "Pursuant to Title 17 Section 105 of the United States " +
      "Code, this file is not subject to copyright protection " +
      "and is in the public domain. For more information " +
      "please see http://www.gpo.gov/help/index.html#" +
      "public_domain_copyright_notice.htm"

    val expected = Seq(default)
    assert(extractor.rights(Document(xml)) === expected)
  }

//  it should "throw exception for invalid rights statement" in {
//    val xml =
//      <record>
//        <metadata>
//          <marc:record>
//            <marc:datafield tag="506">
//              <marc:subfield>Subscription required for access.</marc:subfield>
//            </marc:datafield>
//          </marc:record>
//        </metadata>
//      </record>
//
//    //assert extractor.rights(Document(xml)) will throw exception
//
//  }

  it should "extract the correct subject" in {
    val expected = Seq(
      "Radar operators",
      "United States",
      "Radar air traffic control systems",
      "Aviation medicine",
      "Research"
    ).map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

//  it should "extract the correct temporal" in {
//    val expected = Seq("").map(stringOnlyTimeSpan)
//    assert(extractor.temporal(xml) === expected)
//  }

  it should "extract the correct titles" in {
    val expected = Seq("Physiological, subjective, and performance correlates of reported boredom and monotony while performing a simulated radar control task /")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct types from leader" in {
    val expected = Seq("Text")
    assert(extractor.`type`(xml) === expected)
  }
}
