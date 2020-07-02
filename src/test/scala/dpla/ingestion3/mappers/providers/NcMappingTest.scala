package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class NcMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "digitalnc"
  val xmlString: String = new FlatFileIO().readFileAsString("/nc.xml")
  val badXmlString: String = new FlatFileIO().readFileAsString("/michigan_bad.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new NcMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName())

  it should "extract the correct original identifier" in {
    val expected = Some("urn:brevard.lib.unc.educampbell_p15834coll2:oai:cdm15834.contentdm.oclc.org:p15834coll2/9")
    assert(extractor.originalId(xml) == expected)
  }

  it should "extract the correct collection title" in {
    val expected = Seq("Honorary Awards Citations").map(nameOnlyCollection)
    assert(extractor.collection(xml) == expected)
  }

  it should "extract correct contributor" in {
    val expected = Seq("Contributor").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  it should "extract the correct creators when roleTerm is 'creator'" in {
    val expected = Seq("Creator").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "not extract creator from the subject/name/namePart field" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:subject>
              <mods:name>
                <mods:namePart>Cats</mods:namePart>
              </mods:name>
            </mods:subject>
          </mods>
        </metadata>
      </record>

    assert(extractor.creator(Document(xml)) == Seq())
  }

  it should "extract the correct dates for 1996-05-13" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <originInfo>
                <dateCreated keyDate="yes">1996-05-13</dateCreated>
            </originInfo>
        </mods>
      </metadata>
    </record>

    val expected = Seq(stringOnlyTimeSpan("1996-05-13"))
    assert(expected === extractor.date(Document(xml)))
  }

  it should "extract the correct description from note field" in {
    val expected = Seq("Citation for Denelle Hicks, student recipient of the Algernon Sydney Sullivan Award.")
    assert(extractor.description(xml) == expected)
  }

  it should "extract the correct format from physicalDescription \\ form field" in {
    val expected = Seq("Form")
    assert(extractor.format(xml) === expected)
  }

  it should "extract the correct identifier" in {
    val expected = Seq("http://cdm15834.contentdm.oclc.org/cdm/ref/collection/p15834coll2/id/9")
    assert(extractor.identifier(xml) == expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("eng").map(nameOnlyConcept)
    assert(extractor.language(xml) == expected)
  }

  it should "extract the correct place" in {
    val expected = Seq("Buies Creek (N.C.)").map(nameOnlyPlace)
    assert(extractor.place(xml) == expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("Campbell University").map(nameOnlyAgent)
    assert(extractor.publisher(xml) == expected)
  }

  it should "extract the correct rights when accessCondition has not attributes" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:accessCondition>rights statement.</mods:accessCondition>
          </mods>
        </metadata>
      </record>

    val expected = Seq("rights statement.")
    assert(extractor.rights(Document(xml)) == expected)
  }

  it should "extract the correct rights" in {
    val expected = Seq("Copyright Campbell University. The materials in this collection are made available for use in research, teaching and private study. Images and text may not be used for any commercial purposes without prior permission from Campbell University.")
    assert(extractor.rights(xml) == expected)
  }

  it should "extract the correct subject from subject field" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:subject>
              <topic>Dogs</topic>
            </mods:subject>
          </mods>
        </metadata>
      </record>

    val expected = Seq("Dogs").map(nameOnlyConcept)
    assert(extractor.subject(Document(xml)) == expected)
  }

  it should "extract the correct subject from subject/topic field" in {
    val expected = Seq("Algernon Sydney Sullivan Student Award").map(nameOnlyConcept)
    assert(extractor.subject(xml) == expected)
  }

  it should "extract the correct title" in {
    val expected = Seq("Hicks, Denelle - 1996 Sullivan Student Award Recipient")
    assert(extractor.title(xml) == expected)
  }

  it should "not extract the title from the subject/titleInfo/title field" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:subject>
              <mods:titleInfo>
                <mods:title>Birds</mods:title>
              </mods:titleInfo>
            </mods:subject>
          </mods>
        </metadata>
      </record>

    assert(extractor.title(Document(xml)) == Seq())
  }

  it should "not extract title from relatedItem/titleInfo/title field" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:relatedItem>
              <mods:titleInfo>
                <mods:title>Collection title</mods:title>
              </mods:titleInfo>
            </mods:relatedItem>
          </mods>
        </metadata>
      </record>

    assert(extractor.title(Document(xml)) == Seq())
  }

  it should "extract the correct type" in {
    val expected = Seq("Text")
    assert(extractor.`type`(xml) == expected)
  }

  it should "create the correct DPLA Uri" in {
    val expected = Some(URI("http://dp.la/api/items/98d658c94f84f256048755516f2cb86b"))
    assert(extractor.dplaUri(xml) == expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("Campbell University").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) == expected)
  }

  it should "extract the correct intermediateProvider" in {
    val expected = Some("Intermediate Provider").map(nameOnlyAgent)
    assert(extractor.intermediateProvider(xml) == expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq("http://cdm15834.contentdm.oclc.org/cdm/ref/collection/p15834coll2/id/9")
      .map(stringOnlyWebResource)
    assert(extractor.isShownAt(xml) == expected)
  }

  it should "extract the correct preview" in {
    val expected = Seq("http://cdm15834.contentdm.oclc.org/utils/getthumbnail/collection/p15834coll2/id/9")
      .map(stringOnlyWebResource)
    assert(extractor.preview(xml) == expected)
  }


  it should "extract the correct dataProvider for urn:brevard.lib.unc.eduduke_adaccess:4a533646-09a8-4945-8389-e7a8dc7cbf18" in {
    val xml: Document[NodeSeq] = Document(
      <record xmlns="http://www.openarchives.org/OAI/2.0/">
        <header>
          <identifier>
            urn:brevard.lib.unc.eduduke_adaccess:4a533646-09a8-4945-8389-e7a8dc7cbf18
          </identifier>
          <datestamp>2017-02-20</datestamp>
          <setSpec>duke_adaccess</setSpec>
        </header>
        <metadata>
          <mods xmlns="http://www.loc.gov/mods/v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:dul_dc="https://repository.duke.edu/schemas/dul_dc/" xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/" xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-4.xsd" version="3.4">
            <note type="ownership">Duke University Libraries</note>
          </mods>
        </metadata>
      </record>
    )

    val expected = Seq("Duke University Libraries").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) == expected)
  }

  it should "apply the correct `aviation` tag when the correct value exists in description" in {

    val xml: Document[NodeSeq] = Document(
      <record xmlns="http://www.openarchives.org/OAI/2.0/">
        <metadata>
          <mods>
            <note type="content">This item was digitized as part of the "Cleared to Land" project, supported by a grant from the National Historical Publications &amp; Records Commission (NHPRC).</note>
          </mods>
        </metadata>
      </record>
    )
    val expected = Seq(URI("aviation"))
    assert(extractor.tags(xml) === expected)
  }
}
