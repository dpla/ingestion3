package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

class HathiMappingTest extends AnyFlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "hathi"
  val xmlString: String = new FlatFileIO().readFileAsString("/hathi.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new HathiMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName)

  it should "extract the correct original identifier " in {
    val expected = Some("009420214")
    assert(extractor.originalId(xml) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/0000090ef074502284f365460fc14c42"))
    assert(extractor.dplaUri(xml) === expected)
  }

  it should "extract the correct contributor" in {
    val expected = Seq("Everhart, Benjamin Matlock, 1818-1904.", "Academy of Natural Sciences of Philadelphia.")
      .map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("Ellis, Job Bicknell, 1829-1905.").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct date" in {
    val expected = Seq("[1893]").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract year-year date"in {
    val xml =
      <record>
        <controlfield tag="008">881109m18831887iau 000 0 eng d</controlfield>
      </record>
    val expected = Seq("1883-1887").map(stringOnlyTimeSpan)
    assert(extractor.date(Document(xml)) === expected)
  }

  it should "remove 9999 from end of date range" in {
    val xml =
      <record>
        <controlfield tag="008">750822c18539999nikfrzp 0 a0eng</controlfield>
      </record>
    val expected = Seq("1853").map(stringOnlyTimeSpan)
    assert(extractor.date(Document(xml)) === expected)
  }

  it should "extract the correct description" in {
    val expected =
      Seq("From the Proceedings of The academy of natural science of Philadelphia, 1893.", "A second description.")
    assert(extractor.description(xml) == expected)
  }

  it should "extract the correct extent" in {
    val expected = Seq("1, 128-172 p.")
    assert(extractor.extent(xml) == expected)
  }

  it should "extract the correct format" in {
    val expected = Seq("Language material", "Electronic resource", "unmediated", "volume", "Periodicals.")
    assert(extractor.format(xml) === expected)
  }

  it should "handle multiple formats from control field" in {
    val xml =
      <record>
        <controlfield tag="007">cr bn ---auaua</controlfield>
        <controlfield tag="007">he bmb020baca</controlfield>
      </record>
    val expected = Seq("Electronic resource", "Microform")
    assert(extractor.format(Document(xml)) === expected)
  }

  it should "fail gracefully if leader format key is unexpected" in {
    val xml =
      <record>
        <leader>00737nxm a22002051  4500</leader>
      </record>
    val expected = Seq()
    assert(extractor.format(Document(xml)) === expected)
  }

  it should "fail gracefully if control format key is unexpected" in {
    val xml =
      <record>
        <controlfield tag="007">xr bn ---auaua</controlfield>
      </record>
    val expected = Seq()
    assert(extractor.format(Document(xml)) === expected)
  }

  it should "extract the correct identifier" in {
    val expected = Seq(
      "sdr-pst.a164965",
      "(OCoLC)316829673",
      "LIAS256769",
      "Hathi: 009420214")
    assert(extractor.identifier(xml) == expected)
  }

  it should "extract the correct ISBN identifier" in {
    val xml =
      <record>
        <datafield ind2=" " ind1=" " tag="020">
          <subfield code="a">8436305477 (set)</subfield>
        </datafield>
      </record>
    val expected = Seq("ISBN: 8436305477 (set)")
    assert(extractor.identifier(Document(xml)) == expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("ger", "eng").map(nameOnlyConcept)
    assert(extractor.language(xml) == expected)
  }

  it should "extract the correct place" in {
    val expected = Seq("United States", "North America").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("n.p. : n. pub.,").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract the correct relation" in {
    val expected =
      Seq("Online version:. Howard, Clifford, 1868-1942. What happened at Olenberg. Chicago : The Reilly & Britton Co., 1911. (OCoLC)656701318").map(eitherStringOrUri)
    assert(extractor.relation(xml) === expected)
  }

  it should "extract the correct rights" in {
    val expected = Seq("Public domain only when viewed in the US. Learn more at http://www.hathitrust.org/access_use")
    assert(extractor.rights(xml) === expected)
  }

  it should "fail gracefully if rights key is unexpected" in {
    val xml =
      <record>
        <datafield>
          <subfield code="r">xxx</subfield>
        </datafield>
      </record>
    val expected = Seq()
    assert(extractor.rights(Document(xml)) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq(
      "Fungi--North America",
      "Antiquities",
      "United States",
      "United States--History--Periodicals",
      "United States--Antiquities--Periodicals",
      "Stuarts, 1603-1714",
      "Periodicals",
      "History",
      "History Serials"
    ).map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct temporal" in {
    val expected = Seq("1603 - 1714", "fast", "Stuarts, 1603-1714").map(stringOnlyTimeSpan)
    assert(extractor.temporal(xml) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("New species of North American fungi from various localities /")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct types from leader" in {
    val expected = Seq("Text")
    assert(extractor.`type`(xml) === expected)
  }

  it should "extract the correct types from datafield" in {
    val xml =
      <record>
        <leader></leader>
        <datafield ind2=" " ind1=" " tag="970">
          <subfield code="a">MN</subfield>
        </datafield>
      </record>
    val expected = Seq("Image")
    assert(extractor.`type`(Document(xml)) === expected)
  }

  it should "fail gracefully if leader type key is unexpected" in {
    val xml =
      <record>
        <leader>00737nxx a22002051  4500</leader>
      </record>
    val expected = Seq()
    assert(extractor.`type`(Document(xml)) === expected)
  }

  it should "fail gracefully if datafield type key is unexpected" in {
    val xml =
      <record>
        <leader></leader>
        <datafield ind2=" " ind1=" " tag="970">
          <subfield code="a">xx</subfield>
        </datafield>
      </record>
    val expected = Seq()
    assert(extractor.`type`(Document(xml)) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq(nameOnlyAgent("Penn State University"))
    assert(extractor.dataProvider(xml) === expected)
  }

  it should "fail gracefully if dataProvider key is unexpected" in {
    val xml =
      <record>
        <datafield ind2=" " ind1=" " tag="974">
          <subfield code="u">xxx.000061785779</subfield>
        </datafield>
      </record>
    val expected = Seq()
    assert(extractor.dataProvider(Document(xml)) === expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq(uriOnlyWebResource(URI("http://catalog.hathitrust.org/Record/009420214")))
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract the correct OCLC id" in {
    val xml =
      <record>
        <datafield ind2=" " ind1=" " tag="035">
          <subfield code="a">(OCoLC)39816489</subfield>
        </datafield>
      </record>
    val expected = Some("39816489")
    assert(extractor.oclcId(Document(xml)) === expected)
  }

  it should "extract the correct OCLC id (with letter prefix)" in {
    val xml =
      <record>
        <datafield ind2=" " ind1=" " tag="035">
          <subfield code="a">(OCoLC)ocm13230493</subfield>
        </datafield>
      </record>
    val expected = Some("13230493")
    assert(extractor.oclcId(Document(xml)) === expected)
  }

  it should "extract the correct ISBN id" in {
    val xml =
      <record>
        <datafield ind2=" " ind1=" " tag="020">
          <subfield code="a">8436305477 (set)</subfield>
        </datafield>
      </record>
    val expected = Some("8436305477")
    assert(extractor.isbnId(Document(xml)) === expected)
  }

  it should "extract the correct google prefix" in {
    val xml =
      <record>
        <datafield ind2=" " ind1=" " tag="974">
          <subfield code="u">chi.72963127</subfield>
        </datafield>
        <datafield ind2=" " ind1=" " tag="974">
          <subfield code="u">chi.72963110</subfield>
        </datafield>
      </record>
    val expected = Some("CHI")
    assert(extractor.googlePrefix(Document(xml)) === expected)
  }

  it should "extract the correct google prefix (UCAL)" in {
    val xml =
      <record>
        <datafield ind2=" " ind1=" " tag="974">
          <subfield code="u">uc1.b268676</subfield>
        </datafield>
        <datafield ind2=" " ind1=" " tag="974">
          <subfield code="u">uc2.ark:/13960/t78s4nt84</subfield>
        </datafield>
      </record>
    val expected = Some("UCAL")
    assert(extractor.googlePrefix(Document(xml)) === expected)
  }

  it should "extract the correct google prefix (UCLA)" in {
    val xml =
      <record>
        <datafield ind2=" " ind1=" " tag="974">
          <subfield code="u">uc1.l0064507957</subfield>
        </datafield>
      </record>
    val expected = Some("UCLA")
    assert(extractor.googlePrefix(Document(xml)) === expected)
  }
}
