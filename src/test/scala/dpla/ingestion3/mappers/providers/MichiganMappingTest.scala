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
  val xmlString: String = new FlatFileIO().readFileAsString("/oklahoma.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new MichiganMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName())

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
}
