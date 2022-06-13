package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class MaMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "ma"
  val xmlString: String = new FlatFileIO().readFileAsString("/ma.xml")
  val xmlStringCreator: String = new FlatFileIO().readFileAsString("/ma-creator.xml")

  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val xmlCreator: Document[NodeSeq] = Document(XML.loadString(xmlStringCreator))

  val extractor = new MaMapping

  it should "not use the provider shortname in minting IDs " in
    assert(extractor.useProviderName())

  it should "extract the correct original identifier " in
    assert(extractor.originalId(xml) === Some("oai:digitalcommonwealth.org:commonwealth-oai:0k225b90r"))

  it should "extract the correct alternate titles" in {
    val expected = Seq("Alternate Title", "Kio circus")
    assert(extractor.alternateTitle(xml) === expected)
  }

  it should "extract the correct collection titles" in {
    val expected = Seq("College Archives Digital Collections")
      .map(nameOnlyCollection)
    assert(extractor.collection(xml) === expected)
  }

  it should "extract the correct contributor" in {
    val expected = Seq("American Photo-Relief Printing Co").map(nameOnlyAgent)
    assert(extractor.contributor(xml) === expected)
  }

  it should "extract the correct creators" in {
    val expected = Seq("Alfred, 2000", "John").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct creators from xmlCreators" in {
    val expected = Seq(
      "Bonet, Honoré, active 1378-1398",
      "Moris, V.C.",
      "Caxton, William, approximately 1422-1491 or 1492",
      "Sonnyng, William",
      "Sonnyng, John",
      "Wall, Thomas, 1504-1536",
      "Spelman, Henry, Sir, 1564?-1641",
      "Macro, Cox, -1767",
      "Patteson, John, 1755-1833",
      "Gurney, Hudson, 1775-1864",
      "Gurney, J. H. (John Henry), 1848-1922",
      "Gurney, Q. E. (Quintin Edward), 1883-",
      "Abbaye Saint-Pierre de Hasnon (Hasnon, France)",
      "Maggs Bros."
    ).map(nameOnlyAgent)

    assert(extractor.creator(xmlCreator) == expected)
  }

  it should "extract the correct dates when only given a keyDate" in {
    val xml: Document[NodeSeq] = Document(
      <record>
        <metadata>
          <mods:mods>
            <originInfo>
              <dateCreated keyDate="yes" encoding="w3cdtf">2010-11-31</dateCreated>
            </originInfo>
          </mods:mods>
        </metadata>
      </record>)

    val expected = Seq("2010-11-31").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct dates when given point=start and point=end" in {
    val xml: Document[NodeSeq] = Document(
      <record>
        <metadata>
          <mods:mods>
            <originInfo>
              <mods:dateCreated point="start" encoding="w3cdtf" qualifier="questionable" keyDate="yes">
                1450
              </mods:dateCreated>
              <mods:dateCreated point="end" encoding="w3cdtf" qualifier="questionable">
                1471
              </mods:dateCreated>
            </originInfo>
          </mods:mods>
        </metadata>
      </record>)

    val expected = Seq(
      EdmTimeSpan(
        originalSourceDate = Some("1450-1471"),
        begin = Some("1450"),
        end = Some("1471")
      ))
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("Note", "Abstract"))

  it should "extract the correct extent" in {
    assert(extractor.extent(xml) == Seq("extent"))
  }

  it should "extract the correct format" in {
    val expected = Seq("Photographs")
    assert(extractor.format(xml) === expected)
  }

  it should "extract rights value" in {
    assert(extractor.rights(xml) === Seq("Text and images are owned, held, or licensed by Springfield College and are available for personal, non-commercial, and educational use, provided that ownership is properly cited. A credit line is required and should read: Courtesy of Springfield College, Babson Library, Archives and Special Collections. Any commercial use without written permission from Springfield College is strictly prohibited. Other individuals or entities other than, and in addition to, Springfield College may also own copyrights and other propriety rights. The publishing, exhibiting, or broadcasting party assumes all responsibility for clearing reproduction rights and for any infringement of United States copyright law.", "Contact host institution for more information."))
  }

  it should "extract the correct edmRights " in {
    val expected = Seq(URI("http://rightsstatements.org/vocab/InC-EDU/1.0/"))
    assert(extractor.edmRights(xml) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("Art Linkletter Natatorium at Springfield College")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq(uriOnlyWebResource(URI("http://cdm16122.contentdm.oclc.org/cdm/ref/collection/p15370coll2/id/2977")))
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/a4a613c7e9b833e4b752476869aaeb45"))
    assert(extractor.dplaUri(xml) === expected)
  }

  it should "extract the correct subjects " in {
    val expected = Seq("Linkletter Natatorium", "Springfield College", "Springfield College--Buildings",
    "Triangles", "Springfield (Mass.)", "Swimming pools", "Autumn").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct places" in {
    val xml: Document[NodeSeq] = Document(
      <record>
        <metadata>
          <mods:mods>
            <mods:subject authority="tgn" valueURI="http://vocab.getty.edu/tgn/7013445" authorityURI="http://vocab.getty.edu/tgn/">
              <mods:hierarchicalGeographic>
                <mods:county>Suffolk</mods:county>
                <mods:country>United States</mods:country>
                <mods:state>Massachusetts</mods:state>
                <mods:continent>North and Central America</mods:continent>
                <mods:city>Boston</mods:city>
                <mods:citySection>Back Bay</mods:citySection>
              </mods:hierarchicalGeographic>
              <mods:cartographics>
                <mods:coordinates>42.35,-71.05</mods:coordinates>
              </mods:cartographics>
            </mods:subject>
            <mods:subject authority="tgn" valueURI="http://vocab.getty.edu/tgn/7013445" authorityURI="http://vocab.getty.edu/tgn/">
              <geographic>A Place</geographic>
            </mods:subject>
          </mods:mods>
        </metadata>
      </record>
    )

    val expected = Seq(DplaPlace(
      county = Some("Suffolk"),
      country = Some("United States"),
      state = Some("Massachusetts"),
      city = Some("Boston, Back Bay"),
      coordinates = Some("42.35,-71.05"),
      name = Some("Boston, Back Bay, Suffolk, Massachusetts, United States")
    ), nameOnlyPlace("A Place"))

    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct place for Name/Coord" in {
    val xml: Document[NodeSeq] = Document(
      <record>
        <metadata>
          <mods:mods>
            <mods:subject authority="tgn" valueURI="http://vocab.getty.edu/tgn/7013445" authorityURI="http://vocab.getty.edu/tgn/">
              <geographic>Fenway Park</geographic>
              <mods:cartographics>
                <mods:coordinates>42.35,-71.05</mods:coordinates>
              </mods:cartographics>
            </mods:subject>
            <mods:subject authority="tgn" valueURI="http://vocab.getty.edu/tgn/7013445" authorityURI="http://vocab.getty.edu/tgn/">
              <geographic>A Place</geographic>
            </mods:subject>
          </mods:mods>
        </metadata>
      </record>
    )

    val expected = Seq(DplaPlace(
      coordinates = Some("42.35,-71.05"),
      name = Some("Fenway Park")
    ), nameOnlyPlace("A Place"))

    assert(extractor.place(xml) === expected)
  }

  it should "extract the correct IIIF manifest" in {
    val expected = Seq(URI("https://ark.digitalcommonwealth.org/ark:/50959/v405tb16x/manifest"))
    assert(extractor.iiifManifest(xml) === expected)
  }

  it should "extract the correct rs.org value from xref" in {
    val xml: Document[NodeSeq] = Document(
      <record>
        <metadata>
          <mods:mods>
            <mods:accessCondition displayLabel='rightsstatements.org' xlink:href='http://rightsstatements.org/vocab/NoC-US/1.0/' type='use and reproduction'>
              No Copyright - United States
            </mods:accessCondition>
          </mods:mods>
        </metadata>
      </record>
    )

    val expected = Seq(URI("http://rightsstatements.org/vocab/NoC-US/1.0/"))
    assert(extractor.edmRights(xml) === expected)
  }

  it should "extract the correct identifiers" in {
    val xml: Document[NodeSeq] = Document(
      <record>
        <metadata>
          <mods:mods>
            <mods:identifier type="local-accession">rg131-02-07-006</mods:identifier>
          </mods:mods>
        </metadata>
      </record>
    )

    val expected = Seq("Local accession: rg131-02-07-006")
    assert(extractor.identifier(xml) === expected)

  }
  it should "extract identifiers" in {
    val xml: Document[NodeSeq] = Document(
      <record>
        <metadata>
          <mods:mods>
            <mods:identifier type="local-accession">
              Z2016-20 Sarah (Sallie) Moore Field Collection
            </mods:identifier>
            <mods:identifier type="local-other">
              19030509
            </mods:identifier>
          </mods:mods>
        </metadata>
      </record>
    )

    val expected = Seq("Local accession: Z2016-20 Sarah (Sallie) Moore Field Collection",
    "Local other: 19030509")
    assert(extractor.identifier(xml) === expected)

  }


  it should "extract the correct multiple identifiers" in {
    val xml: Document[NodeSeq] = Document(
      <record>
        <metadata>
          <mods:mods>
            <mods:identifier type="local-accession">rg131-02-07-006</mods:identifier>
            <mods:identifier type="local-accession">rg131-02-07-007</mods:identifier>
          </mods:mods>
        </metadata>
      </record>
    )

    val expected = Seq("Local accession: rg131-02-07-006, rg131-02-07-007")
    assert(extractor.identifier(xml) === expected)

  }

  it should "extract the correct multiple identifiers multiple types" in {
    val xml: Document[NodeSeq] = Document(
      <record>
        <metadata>
          <mods:mods>
            <mods:identifier type="local-accession">rg131-02-07-006</mods:identifier>
            <mods:identifier type="local-accession">rg131-02-07-007</mods:identifier>
            <mods:identifier type="local-other">andg-43=343</mods:identifier>
          </mods:mods>
        </metadata>
      </record>
    )

    val expected = Seq("Local accession: rg131-02-07-006, rg131-02-07-007", "Local other: andg-43=343")
    assert(extractor.identifier(xml) === expected)

  }
}
