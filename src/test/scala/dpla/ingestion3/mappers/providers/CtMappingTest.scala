package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}
import dpla.ingestion3.model._
import scala.xml.{NodeSeq, XML}

class CtMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "ct"
  val xmlString: String = new FlatFileIO().readFileAsString("/ct.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new CtMapping

  it should "not use the provider shortname in minting IDs " in
    assert(!extractor.useProviderName)

  it should "extract the correct original ID" in {
    val expected = Some("http://hdl.handle.net/11134/20002:1323")
    assert(extractor.originalId(xml) == expected)
  }

  it should "extract the correct alternate titles " in {
    val expected_1 = "Law concerning the reunion of Austria with the German Reich of 13 March 1938"
    val expected_2 = "Translation of Document 2307-PS"
    val expected_3 = "nonsort title subtitle partname partnumber"

    assert(extractor.alternateTitle(xml).contains(expected_1))
    assert(extractor.alternateTitle(xml).contains(expected_2))
    assert(extractor.alternateTitle(xml).contains(expected_3))
  }

  it should "extract the correct contributors" in {
    val expected = Seq(
      "Dodd, Thomas J. (Thomas Joseph), 1907-1971",
      "International Military Tribunal",
      "Germany. Laws, etc. (Reichsgesetzblatt)"
    ).map(nameOnlyAgent)

    assert(extractor.contributor(xml) === expected)
  }

  it should "extract the correct creators" in {
    val expected = Seq("Creator").map(nameOnlyAgent)
    assert(extractor.creator(xml) === expected)
  }

  it should "extract the correct dates" in {
    val expected = Seq("1938").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract and construct the correct dates from start and end attributes" in {
    val xml = <mods><originInfo start="1938" end="1939"></originInfo></mods>
    val expected = Seq("1938-1939").map(stringOnlyTimeSpan)
    assert(extractor.date(Document(xml)) === expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("The Common Plan or Conspiracy (Boxes 282-284) refers to Count I of the IMT indictment, namely conspiracy to commit unlawful aggression. This subseries consists of trial briefs and German documents used by the Allied prosecution team. The papers are arranged thematically as the conspiracy charge had economic, political, and military dimensions. This subseries includes detailed German war plans for invading such nations as Poland, France, Russia, Norway, and Yugoslavia. The minutes of high-level economic meetings on the mobilization for war also highlight these files. Includes: English and German language version of Document no. 2307-PS. Annotated.")
    assert(extractor.description(xml) === expected)
  }

  it should "extract the correct extent values" in {
    val xml = <mods><physicalDescription><extent unit="unitVal">extent</extent></physicalDescription></mods>
    val expected = Seq("extent unitVal")
    assert(extractor.extent(Document(xml)) === expected)
  }

  it should "extract the correct extent values if only unit given" in {
    val xml = <mods><physicalDescription><extent unit="unitVal"></extent></physicalDescription></mods>
    val expected = Seq("unitVal")
    assert(extractor.extent(Document(xml)) === expected)
  }

  it should "extract the correct extent values if none given" in {
    val expected = Seq()
    assert(extractor.extent(xml) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("Anschluss")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = List(stringOnlyWebResource("http://hdl.handle.net/11134/20002:1323"))
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "extract the correct preview" in {
    val expected = List(stringOnlyWebResource("https://ctdigitalarchive.org/islandora/object/20002:1323/datastream/TN"))
    assert(extractor.preview(xml) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/92fc087881322d8223ba36a9b25b66e8"))
    assert(extractor.dplaUri(xml) === expected)
  }

  it should "extract the correct edmRights value" in {
    val expected = Seq(URI("rs.org"))
    assert(extractor.edmRights(xml) === expected)
  }

  it should "not extract xlink:href to edmRights when displayLabel attribute present" in {
    val xml: Document[NodeSeq] = Document(
      <mods>
        <accessCondition type="use and reproduction" displayLabel="public" xlink:href="uconn.edu/rights">
          This work is licensed under a Creative Commons Attribution-NonCommercial 4.0 International License, CC BY-NC
          .</accessCondition>
      </mods>
    )
    val expected = Seq()
    assert(extractor.edmRights(xml) === expected)
  }
}