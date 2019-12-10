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

  it should "extract the correct contributor" in {
    val xml =
      <record>
        <metadata>
          <marc:record>
            <marc:datafield ind2=" " ind1="1" tag="700">
              <marc:subfield code="a">Rezey, Maribeth L.,</marc:subfield>
              <marc:subfield code="e">author.</marc:subfield>
            </marc:datafield>
            <marc:datafield ind2=" " ind1="1" tag="710">
              <marc:subfield code="a">United States.</marc:subfield>
              <marc:subfield code="b">Bureau of Justice Statistics,</marc:subfield>
              <marc:subfield code="e">issuing body.</marc:subfield>
            </marc:datafield>
          </marc:record>
        </metadata>
      </record>
    val expected = Seq("United States. Bureau of Justice Statistics, issuing body.")
      .map(nameOnlyAgent)
    assert(extractor.contributor(Document(xml)) == expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("Thackray, Richard I.").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct date" in {
    val expected = Seq("1975").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct controlfield date" in {
    val xml =
      <record>
        <metadata>
          <marc:record>
            <marc:controlfield tag="008">120815c20129999mdu x w o    f0    2eng c</marc:controlfield>
          </marc:record>
        </metadata>
      </record>

    val expected = Seq("2012").map(stringOnlyTimeSpan)
    assert(extractor.date(Document(xml)) == expected)
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

  it should "extract the correct language (free text)" in {
    val xml =
      <record>
        <metadata>
          <marc:record>
            <marc:datafield ind2=" " ind1=" " tag="546">
              <marc:subfield code="a">Text in Spanish.</marc:subfield>
            </marc:datafield>
          </marc:record>
        </metadata>
      </record>
    val expected = Seq("Text in Spanish.").map(nameOnlyConcept)
    assert(extractor.language(Document(xml)) == expected)
  }

  it should "extract the correct language (code)" in {
    val xml =
      <record>
        <metadata>
          <marc:record>
            <marc:datafield ind2=" " ind1="0" tag="041">
              <marc:subfield code="a">eng</marc:subfield>
              <marc:subfield code="a">spa</marc:subfield>
            </marc:datafield>
          </marc:record>
        </metadata>
      </record>
    val expected = Seq("eng", "spa").map(nameOnlyConcept)
    assert(extractor.language(Document(xml)) == expected)
  }

  it should "extract the correct place" in {
    val expected = Seq("United States").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("Washington, D.C. : U.S. Dept. of Transportation, Federal Aviation Administration, Office of Aviation Medicine,").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract the correct relation" in {
    val xml =
      <record>
        <metadata>
          <marc:record>
            <marc:datafield ind2=" " ind1="1" tag="490">
              <marc:subfield code="a">DHHS (NIOSH) publication ;</marc:subfield>
              <marc:subfield code="v">no. 90-100</marc:subfield>
            </marc:datafield>
          </marc:record>
        </metadata>
      </record>
    val expected =
      Seq("DHHS (NIOSH) publication ;. no. 90-100.").map(eitherStringOrUri)
    assert(extractor.relation(Document(xml)) === expected)
  }

  it should "extract the correct rights" in {
    val xml =
      <record>
        <metadata>
          <marc:record>
            <marc:datafield ind2=" " ind1=" " tag="506">
              <marc:subfield code="a">For official use only, v. 3.</marc:subfield>
            </marc:datafield>
          </marc:record>
        </metadata>
      </record>
    val expected = Seq("For official use only, v. 3.")
    assert(extractor.rights(Document(xml)) === expected)
  }

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

  it should "throw exception for invalid rights statement" in {
    val xml =
      <record>
        <metadata>
          <marc:record>
            <marc:datafield tag="506">
              <marc:subfield>Subscription required for access.</marc:subfield>
            </marc:datafield>
          </marc:record>
        </metadata>
      </record>

    assertThrows[Exception] { extractor.rights(Document(xml)) }
  }

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

  it should "extract the correct temporal" in {
    val xml =
      <record>
        <metadata>
          <marc:record>
            <marc:datafield ind2="0" ind1="2" tag="611">
              <marc:subfield code="a">Olympic Winter Games</marc:subfield>
              <marc:subfield code="n">(19th :</marc:subfield>
              <marc:subfield code="d">2002 :</marc:subfield>
              <marc:subfield code="c">Salt Lake City, Utah)</marc:subfield>
            </marc:datafield>
          </marc:record>
        </metadata>
      </record>
    val expected = Seq("2002 :").map(stringOnlyTimeSpan)
    assert(extractor.temporal(Document(xml)) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("Physiological, subjective, and performance correlates of reported boredom and monotony while performing a simulated radar control task /")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct types from leader" in {
    val expected = Seq("Text")
    assert(extractor.`type`(xml) === expected)
  }
}
