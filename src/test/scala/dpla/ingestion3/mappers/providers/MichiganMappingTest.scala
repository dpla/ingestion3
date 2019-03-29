package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class MichiganMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "mi"
  val xmlString: String = new FlatFileIO().readFileAsString("/michigan.xml")
  val badXmlString: String = new FlatFileIO().readFileAsString("/michigan_bad.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new MichiganMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName())

  it should "extract the correct original identifier" in {
    val expected = Some("oai:michigan:ochr:oai:digital.library.wayne.edu:ochr:oai:demo.ptfs.com:library10_lib/87490")
    assert(extractor.originalId(xml) == expected)
  }

  it should "extract the correct collection title" in {
    val expected = Seq("Veterans History Project interviews, RHC-27").map(nameOnlyCollection)
    assert(extractor.collection(xml) == expected)
  }

  it should "extract correct contributor" in {
    val expected = Seq("Boston Cooking School (Boston, Mass.)").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  it should "extract the correct creators when roleTerm is 'creator'" in {
    val expected = Seq("City of Lansing", "City Planning Division Ingells, Norris").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct creator when roleTerm is blank" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:name>
              <mods:namePart>City of Lansing</mods:namePart>
            </mods:name>
          </mods>
        </metadata>
      </record>
    val expected = Seq("City of Lansing").map(nameOnlyAgent)
    assert(extractor.creator(Document(xml)) == expected)
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

  it should "extract the correct dates for 2007" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <originInfo>
                <dateCreated>2007</dateCreated>
            </originInfo>
        </mods>
      </metadata>
    </record>

    val expected = Seq(stringOnlyTimeSpan("2007"))
    assert(expected === extractor.date(Document(xml)))
  }

  it should "extract the correct dates for start=2007 end=2008" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <originInfo>
                <dateIssued point="start">2007</dateIssued>
                <dateIssued point="end">2008</dateIssued>
            </originInfo>
        </mods>
      </metadata>
    </record>

    val expected = Seq(
      EdmTimeSpan(originalSourceDate = Some("2007-2008"),
        begin = Some("2007"),
        end = Some("2008")
      )
    )
    assert(expected === extractor.date(Document(xml)))
  }

  it should "extract the correct dates when dateCreated, dateIssued and dateIssued start/end exist" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <originInfo>
                <dateCreated>2003</dateCreated>
                <dateIssued>2004</dateIssued>
                <dateIssued point="start">2007</dateIssued>
                <dateIssued point="end">2008</dateIssued>
            </originInfo>
        </mods>
      </metadata>
    </record>

    val expected = Seq(stringOnlyTimeSpan("2003"))
    assert(expected === extractor.date(Document(xml)))
  }

  it should "extract the correct dates when dateCreated and dateIssued exist" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <originInfo>
                <dateCreated>2003</dateCreated>
                <dateCreated>2005</dateCreated>
                <dateIssued>2004</dateIssued>
            </originInfo>
        </mods>
      </metadata>
    </record>

    val expected = Seq(stringOnlyTimeSpan("2003"), stringOnlyTimeSpan("2005"))
    assert(expected === extractor.date(Document(xml)))
  }

  it should "extract the correct description from abstract field" in {
    val expected = Seq( "Avon Township Offices, located on the northwest corner of Fourth and Pine Streets.")
    assert(extractor.description(xml) === expected)
  }

  it should "extract the correct description from note field" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:note>Oakland County Historical Resources hosts digitized materials.</mods:note>
          </mods>
        </metadata>
      </record>

    val expected = Seq("Oakland County Historical Resources hosts digitized materials.")
    assert(extractor.description(Document(xml)) == expected)
  }

  it should "extract the correct description from physicalDescription/note field" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:physicalDescription>
              <mods:note>jpeg</mods:note>
            </mods:physicalDescription>
          </mods>
        </metadata>
      </record>

    val expected = Seq("jpeg")
    assert(extractor.description(Document(xml)) == expected)
  }

  it should "extract the correct extent" in {
    val expected = Seq("1 Postcard (3.5\" x 5.5\")")
    assert(extractor.extent(xml) == expected)
  }

  it should "extract the correct format from physicalDescription field" in {
    val expected = Seq("Postcard")
    assert(extractor.format(xml) === expected)
  }

  it should "extract the correct format from genre field" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:genre authority="dct">still image</mods:genre>
          </mods>
        </metadata>
      </record>

    val expected = Seq("still image")
    assert(extractor.format(Document(xml)) == expected)
  }

  it should "not extract format from the subject/genre field" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:subject>
              <mods:genre>Cows</mods:genre>
            </mods:subject>
          </mods>
        </metadata>
      </record>

    assert(extractor.format(Document(xml)) == Seq())
  }

  it should "extract the correct identifier" in {
    val expected = Seq("MICH1306517")
    assert(extractor.identifier(xml) == expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("eng").map(nameOnlyConcept)
    assert(extractor.language(xml) == expected)
  }

  it should "extract the correct place" in {
    val expected = Seq("407 Pine Street", "Rochester").map(nameOnlyPlace)
    assert(extractor.place(xml) == expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("Grand Valley State University. University Libraries.").map(nameOnlyAgent)
    assert(extractor.publisher(xml) == expected)
  }

  it should "extract the correct rights" in {
    val expected = Seq("Users can cite and link to these materials without obtaining permission.")
    assert(extractor.rights(xml) == expected)
  }

  it should "extract the correct subject from subject field" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:subject>Dogs</mods:subject>
          </mods>
        </metadata>
      </record>

    val expected = Seq("Dogs").map(nameOnlyConcept)
    assert(extractor.subject(Document(xml)) == expected)
  }

  it should "extract the correct subject from subject/topic field" in {
    val expected = Seq("Government").map(nameOnlyConcept)
    assert(extractor.subject(xml) == expected)
  }

  it should "extract the correct subject from subject/name/namePart field" in {
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

    val expected = Seq("Cats").map(nameOnlyConcept)
    assert(extractor.subject(Document(xml)) == expected)
  }

  it should "extract the correct subject from subject/genre field" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:subject>
              <mods:genre>Cows</mods:genre>
            </mods:subject>
          </mods>
        </metadata>
      </record>

    val expected = Seq("Cows").map(nameOnlyConcept)
    assert(extractor.subject(Document(xml)) == expected)
  }

  it should "extract the correct subject from subject/titleInfo/title field" in {
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

    val expected = Seq("Birds").map(nameOnlyConcept)
    assert(extractor.subject(Document(xml)) == expected)
  }

  it should "not extract subject from the subject/geographic field" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:subject>
              <mods:geographic>
                Ann Arbor
              </mods:geographic>
            </mods:subject>
          </mods>
        </metadata>
      </record>

    assert(extractor.subject(Document(xml)) == Seq())
  }

  it should "not extract subject from the subject/temporal field" in {
    val xml =
      <record>
        <metadata>
          <mods>
            <mods:subject>
              <mods:temporal>
                1984-1985
              </mods:temporal>
            </mods:subject>
          </mods>
        </metadata>
      </record>

    assert(extractor.subject(Document(xml)) == Seq())
  }

  it should "extract the correct temporal" in {
    val expected = Seq("1967-2011").map(stringOnlyTimeSpan)
    assert(extractor.temporal(xml) == expected)
  }

  it should "extract the correct title" in {
    val expected = Seq("Avon Township Offices")
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
    val expected = Seq("still image")
    assert(extractor.`type`(xml) == expected)
  }

  it should "create the correct DPLA Uri" in {
    val expected = Some(new URI("http://dp.la/api/items/dfcebfc0d4ce4e47845dd76dc5a86b23"))
    assert(extractor.dplaUri(xml) == expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("Oakland County Historical Resources").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) == expected)
  }

  it should "extract the correct intermediateProvider" in {
    val expected = Some("Rochester Hills Public Library").map(nameOnlyAgent)
    assert(extractor.intermediateProvider(xml) == expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq("http://oaklandcountyhistory.org/knowvation/app/consolidatedSearch/#autologin/guest/lb_document_id=87490")
      .map(stringOnlyWebResource)
    assert(extractor.isShownAt(xml) == expected)
  }

  it should "extract the correct preview" in {
    val expected = Seq("http://oaklandcountyhistory.org/awweb/pdfopener?fp=auto&umb=87490")
      .map(stringOnlyWebResource)
    assert(extractor.preview(xml) == expected)
  }
}
