package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.NodeSeq

class P2PMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]

  val mapping = new P2PMapping

  "A P2PMapping" should "have the correct provider name" in {
    assert(mapping.getProviderName === "p2p")
  }

  it should "get the correct original ID" in {
    val result = mapping.originalId(
      header(
        <identifier>
          oai:plains2peaks:Pine_River_2019-01:oai:prlibrary.cvlcollections.org:54
        </identifier>
      )
    )
    assert(result === Some("oai:plains2peaks:Pine_River_2019-01:oai:prlibrary.cvlcollections.org:54"))
  }

  it should "create the correct DPLA URI" in {
    val result = mapping.dplaUri(
      header(
        <identifier>
          oai:plains2peaks:Pine_River_2019-01:oai:prlibrary.cvlcollections.org:54
        </identifier>
      )
    )
    val expected = Some(URI("http://dp.la/api/items/9314d4b80e857cbc478d9c7d281fd14e"))
    assert(result === expected)
  }

  it should "return the correct data provider" in {
    val result = mapping.dataProvider(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <note type="ownership">Foo</note>
        </mods:mods>
      )
    ).headOption.getOrElse(EdmAgent()).name

    assert(result === Some("Foo"))
  }

  it should "return the correct intermediate provider" in {
    val result = mapping.intermediateProvider(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:note type="admin">Foo</mods:note>
        </mods:mods>
      )
    ).headOption.getOrElse(EdmAgent()).name
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
    ).headOption.getOrElse(EdmWebResource(uri = URI("")))
    assert(result === URI("http://rightsstatements.org/vocab/CNE/1.0/"))
  }

  it should "return the correct isShownAt" in {
    val result = mapping.isShownAt(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:location>
            <mods:url usage="primary display">http://digital.denverlibrary.org/utils/getthumbnail/collection/p15330coll22/id/75547</mods:url>
          </mods:location>
        </mods:mods>
      )
    ).headOption.getOrElse(EdmWebResource(uri = URI(""))).uri
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
    ).headOption.getOrElse(EdmWebResource(uri = URI(""))).uri
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
    ).headOption.getOrElse(EdmAgent()).name
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
    ).headOption.getOrElse(EdmAgent()).name
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
    ).headOption.getOrElse(EdmTimeSpan()).originalSourceDate
    assert(result === Some("[1890-1900?]"))
  }

  it should "extract descriptions" in {
    val result = mapping.description(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:abstract>Studio portrait of a boy dressed in a tailored wool pinstripe suit with a jacket, cut out at the waist, and a skirt. The jacket has decorative braided cord frogs. He wears stockings and high leather shoes with buttons. He holds a cane with a carved handle and leans on a cement chair or bench.</mods:abstract>
        </mods:mods>
      )
    ).headOption.getOrElse("")
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
    ).headOption.getOrElse("")
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
    ).headOption.getOrElse("")
    assert(result === "1 photographic print on card mount : albumen ; 21 x 10 cm. (8 1/2 x 4 in.)")
  }

  it should "extract language" in {
    val result = mapping.language(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:language>
            <mods:languageTerm>English</mods:languageTerm>
          </mods:language>
        </mods:mods>
      )
    ).headOption.getOrElse(SkosConcept()).providedLabel
    assert(result === Some("English"))
  }

  it should "extract subject" in {
    val result = mapping.subject(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:subject>
            <mods:topic>Clothing &amp; dress--19th century</mods:topic>
          </mods:subject>
          <mods:subject>
            <mods:name>Milton Bradley</mods:name>
          </mods:subject>
          <mods:subject>
            <mods:genre>Funky outfits</mods:genre>
          </mods:subject>
        </mods:mods>
      )
    ).map(_.providedLabel.getOrElse(""))
    assert(result.contains("Clothing & dress--19th century"))
    assert(result.contains("Milton Bradley"))
    assert(result.contains("Funky outfits"))
  }

  it should "extract title" in {
    val result = mapping.title(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:titleInfo>
            <mods:title>The English Paitent</mods:title>
          </mods:titleInfo>
        </mods:mods>
      )
    ).headOption.getOrElse("")
    assert(result === "The English Paitent")
  }


  it should "extract type" in {
    val result = mapping.`type`(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:typeOfResource>Image</mods:typeOfResource>
        </mods:mods>
      )
    ).headOption.getOrElse("")
    assert(result === "Image")
  }

  it should "extract publisher" in {
    val result = mapping.publisher(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:originInfo>
            <mods:publisher>The New York Times</mods:publisher>
          </mods:originInfo>
        </mods:mods>
      )
    ).headOption.getOrElse(EdmAgent()).name.getOrElse("")
    assert(result === "The New York Times")
  }

  it should "extract format" in {
    val result = mapping.format(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:physicalDescription>
            <mods:form>Tubular Bells</mods:form>
          </mods:physicalDescription>
        </mods:mods>
      )
    ).headOption.getOrElse("")
    assert(result === "Tubular Bells")
  }

  it should "extract place" in {
    val result = mapping.place(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:subject>
            <mods:geographic>Bag End</mods:geographic>
          </mods:subject>
        </mods:mods>
      )
    ).headOption.getOrElse(DplaPlace()).name.getOrElse("")
    assert(result === "Bag End")
  }

  it should "extract relation" in {
    val result = mapping.relation(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:relatedItem type="series">
            <mods:titleInfo>
              <mods:title>Game of Thrones</mods:title>
            </mods:titleInfo>
          </mods:relatedItem>
        </mods:mods>
      )
    ).headOption.getOrElse(Left(""))
    assert(result === Left("Game of Thrones"))
  }

  it should "extract collection" in {
    val result = mapping.collection(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:relatedItem type="host">
            <mods:titleInfo>
              <mods:title>HBO Videos</mods:title>
            </mods:titleInfo>
          </mods:relatedItem>
        </mods:mods>
      )
    ).headOption.getOrElse(DcmiTypeCollection()).title.getOrElse("")
    assert(result === "HBO Videos")
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