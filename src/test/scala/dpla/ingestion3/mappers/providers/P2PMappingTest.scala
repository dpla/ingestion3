package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model.{EdmAgent, URI}
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.NodeSeq

class P2PMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]

  val mapping = new P2PMapping

  "A P2PMapping" should "have the correct provider name" in {
    assert(mapping.getProviderName === "p2p")
  }

  it should "get the correct provider ID" in {
    val result = mapping.getProviderId(
      header(
        <identifier>
          oai:plains2peaks:Pine_River_2019-01:oai:prlibrary.cvlcollections.org:54
        </identifier>
      )
    )
    assert(result === "oai:plains2peaks:Pine_River_2019-01:oai:prlibrary.cvlcollections.org:54")
  }

  //TODO
  it should "mint the correct DPLA URI" in {
  }

  it should "return the correct data provider" in {
    val result = mapping.dataProvider(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <note type="ownership">Foo</note>
        </mods:mods>
      )
    ).head.name

    assert(result === Some("Foo"))
  }

  it should "return the correct intermediate provider" in {
    val result = mapping.intermediateProvider(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:note type="admin">Foo</mods:note>
        </mods:mods>
      )
    ).head.name
    assert(result === Some("Foo"))
  }

  it should "return the correct edmRights URI" in {
    val result = mapping.edmRights(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:accessCondition type="use and reproduction">
            http://rightsstatements.org/vocab/CNE/1.0/
          </mods:accessCondition>
        </mods:mods>
      )
    ).head
    assert(result === URI("http://rightsstatements.org/vocab/CNE/1.0/"))
  }

it should "return the correct isShownAt" in {
    val result = mapping.isShownAt(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:location>
            <mods:url access="object in context" usage="primary display">http://digital.denverlibrary.org/utils/getthumbnail/collection/p15330coll22/id/75547</mods:url>
          </mods:location>
        </mods:mods>
      )
    ).head.uri
    assert(result === URI("http://digital.denverlibrary.org/utils/getthumbnail/collection/p15330coll22/id/75547"))
  }

  it should "return the originalRecord" in {
    val result = mapping.originalRecord(metadata(Seq()))
    assert(result.contains("<record"))
  }

  it should "return the preview" in {
    val result = mapping.preview(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:location>
            <mods:url access="preview">http://cdm16079.contentdm.oclc.org/cdm/ref/collection/p15330coll22/id/75547</mods:url>
          </mods:location>
        </mods:mods>
      )
    ).head.uri
    assert(result === URI("http://cdm16079.contentdm.oclc.org/cdm/ref/collection/p15330coll22/id/75547"))
  }

  it should "return the provider" in {
    val result = mapping.provider(metadata(Seq()))
    assert(result ===
      EdmAgent(
        name = Some("Plains to Peaks Collective"),
        uri = Some(URI("http://dp.la/api/contributor/p2p"))
      )
    )
  }

  it should "build a sidecar" in {
    //todo
  }

  it should "extract a contributor" in {
    val result = mapping.contributor(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:name>
            <mods:namePart>Rinehart, A. E. (Alfred Evans)</mods:namePart>
            <mods:role>
              <mods:roleTerm type="text">contributor</mods:roleTerm>
            </mods:role>
          </mods:name>
        </mods:mods>
      )
    ).head.name
    assert(result === Some("Rinehart, A. E. (Alfred Evans)"))
  }

  it should "extract a creator" in {
    val result = mapping.creator(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:name>
            <mods:namePart>Rinehart, A. E. (Alfred Evans)</mods:namePart>
            <mods:role>
              <mods:roleTerm type="text">creator</mods:roleTerm>
            </mods:role>
          </mods:name>
        </mods:mods>
      )
    ).head.name
    assert(result === Some("Rinehart, A. E. (Alfred Evans)"))
  }

  it should "extract dates" in {
    val result = mapping.date(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:originInfo>
            <mods:dateCreated keyDate="yes">[1890-1900?]</mods:dateCreated>
          </mods:originInfo>
        </mods:mods>
      )
    ).head.originalSourceDate
    assert(result === Some("[1890-1900?]"))
  }

  it should "extract descriptions" in {
    val result = mapping.description(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:abstract>Studio portrait of a boy dressed in a tailored wool pinstripe suit with a jacket, cut out at the waist, and a skirt. The jacket has decorative braided cord frogs. He wears stockings and high leather shoes with buttons. He holds a cane with a carved handle and leans on a cement chair or bench.</mods:abstract>
        </mods:mods>
      )
    ).head
    assert(result === "Studio portrait of a boy dressed in a tailored wool pinstripe suit with a jacket, cut out at the waist, and a skirt. The jacket has decorative braided cord frogs. He wears stockings and high leather shoes with buttons. He holds a cane with a carved handle and leans on a cement chair or bench.")
  }

  it should "extract extent" in {
    val result = mapping.extent(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:physicalDescription>
            <mods:extent>1 photographic print on card mount : albumen ; 21 x 10 cm. (8 1/2 x 4 in.)</mods:extent>
          </mods:physicalDescription>
        </mods:mods>
      )
    ).head
    assert(result === "1 photographic print on card mount : albumen ; 21 x 10 cm. (8 1/2 x 4 in.)")
  }

  it should "extract identifier" in {
    val result = mapping.extent(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:physicalDescription>
            <mods:extent>1 photographic print on card mount : albumen ; 21 x 10 cm. (8 1/2 x 4 in.)</mods:extent>
          </mods:physicalDescription>
        </mods:mods>
      )
    ).head
    assert(result === "1 photographic print on card mount : albumen ; 21 x 10 cm. (8 1/2 x 4 in.)")
  }




  def metadata(metadata: NodeSeq) = record(Seq(), metadata)

  def header(header: NodeSeq) = record(header, Seq())

  def record(header: NodeSeq, metadata: NodeSeq): Document[NodeSeq] =
    Document(
      <record
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns="http://www.openarchives.org/OAI/2.0/">
        <header>
          {header}
        </header>
        <metadata>
          {metadata}
        </metadata>
      </record>
    )
}