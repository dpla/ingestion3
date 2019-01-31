package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import org.scalatest.{BeforeAndAfter, FlatSpec, FunSuite}

import scala.xml.NodeSeq

class HarvardMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]

  val mapping = new HarvardMapping

  "A Harvard mapping" should "have the correct provider name" in {
    assert(mapping.getProviderName === "harvard")
  }

  it should "get the correct original ID" in {
    val result = mapping.originalId(
      header(
        <identifier>990000915260203941</identifier>
      )
    )
    assert(result === Some("990000915260203941"))
  }

  it should "create the correct DPLA URI" in {
    val result = mapping.dplaUri(
      header(
        <identifier>990000915260203941</identifier>
      )
    )
    val expected = Some(URI("http://dp.la/api/items/7fd6f05e2d23801cb1bff577e6df5f14"))
    assert(result === expected)
  }

  it should "return the correct data provider" in {
    val result = mapping.dataProvider(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:location>
            <mods:physicalLocation type="repository" displayLabel="Harvard repository">
              Master Microforms, Harvard University
            </mods:physicalLocation>
          </mods:location>
        </mods:mods>
      )
    ).headOption.getOrElse(EdmAgent()).name

    assert(result === Some("Master Microforms, Harvard University"))
  }

  it should "return the correct rights statement" in {
    //doesn't matter, canned right now
    val result = mapping.rights(metadata(<modsorsomething/>))
    assert(result === Seq("Held in the collections of Harvard University."))
  }

  it should "return the correct isShownAt" in {
    val results = mapping.isShownAt(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:location>
            <mods:url access="raw object" note="Provides access to page images of entire work">
              https://nrs.harvard.edu/urn-3:FHCL:923450
            </mods:url>
            <mods:url access="preview">https://ids.lib.harvard.edu/ids/view/4965202?width=150&amp;height=150&amp;usethumb=y</mods:url>
            <mods:url access="object in context" displayLabel="Harvard Digital Collections">
              https://id.lib.harvard.edu/digital_collections/990000915260203941
            </mods:url>
            <mods:url access="object in context" displayLabel="Latin American Pamphlet Digital Collection">
              https://id.lib.harvard.edu/curiosity/latin-american-pamphlet-digital-collection/43-990000915260203941
            </mods:url>
          </mods:location>
        </mods:mods>
      )
    ).map(_.uri)
    assert(results.contains(URI("https://id.lib.harvard.edu/digital_collections/990000915260203941")))
    assert(results.contains(URI("https://id.lib.harvard.edu/curiosity/latin-american-pamphlet-digital-collection/43-990000915260203941")))

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
            <mods:url access="raw object" note="Provides access to page images of entire work">
              https://nrs.harvard.edu/urn-3:FHCL:923450
            </mods:url>
            <mods:url access="preview">https://ids.lib.harvard.edu/ids/view/4965202?width=150&amp;height=150&amp;usethumb=y</mods:url>
            <mods:url access="object in context" displayLabel="Harvard Digital Collections">
              https://id.lib.harvard.edu/digital_collections/990000915260203941
            </mods:url>
            <mods:url access="object in context" displayLabel="Latin American Pamphlet Digital Collection">
              https://id.lib.harvard.edu/curiosity/latin-american-pamphlet-digital-collection/43-990000915260203941
            </mods:url>
          </mods:location>
        </mods:mods>
      )
    ).headOption.getOrElse(EdmWebResource(uri = URI(""))).uri
    assert(result === URI("https://ids.lib.harvard.edu/ids/view/4965202?width=150&height=150&usethumb=y"))
  }

  it should "return the provider" in {
    val result = mapping.provider(metadata(Seq()))
    assert(result ===
      EdmAgent(
        name = Some("Harvard Library"),
        uri = Some(URI("http://dp.la/api/contributor/harvard"))
      )
    )
  }

  it should "extract a contributor" in {
    //NOTE: I ginned this one up from a creator example.
    val result = mapping.contributor(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:name type="personal">
            <mods:namePart>Limardo, Ricardo Ovidio.</mods:namePart>
            <mods:role>
              <mods:roleTerm type="text">contributor</mods:roleTerm>
            </mods:role>
          </mods:name>
        </mods:mods>
      )
    ).headOption.getOrElse(EdmAgent()).name
    assert(result === Some("Limardo, Ricardo Ovidio."))
  }

  it should "extract a creator" in {
    val result = mapping.creator(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:name type="personal">
            <mods:namePart>Limardo, Ricardo Ovidio.</mods:namePart>
            <mods:role>
              <mods:roleTerm type="text">creator</mods:roleTerm>
            </mods:role>
          </mods:name>
        </mods:mods>
      )
    ).headOption.getOrElse(EdmAgent()).name
    assert(result === Some("Limardo, Ricardo Ovidio."))
  }

  it should "extract dates" in {
    val result = mapping.date(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:originInfo>
            <mods:place>
              <mods:placeTerm type="code" authority="marccountry">ve</mods:placeTerm>
              <mods:placeTerm type="text" authority="marccountry">Venezuela</mods:placeTerm>
            </mods:place>
            <mods:place>
              <mods:placeTerm type="text">Caracas</mods:placeTerm>
            </mods:place>
            <mods:publisher>Tip. Editorial de &quot;El Avisador Comercial&quot;</mods:publisher>
            <mods:dateIssued>1888</mods:dateIssued>
            <mods:issuance>monographic</mods:issuance>
          </mods:originInfo>
        </mods:mods>
      )
    ).headOption.getOrElse(EdmTimeSpan()).originalSourceDate
    assert(result === Some("1888"))
  }

  //TODO
  ignore should "extract descriptions" in {
    val result = mapping.description(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        </mods:mods>
      )
    ).headOption.getOrElse("")
    assert(false) //result === "")
  }

  it should "extract extent" in {
    val result = mapping.extent(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:physicalDescription>
            <mods:form authority="marcform">print</mods:form>
            <mods:extent>74 p. ; 23 cm.</mods:extent>
          </mods:physicalDescription>
        </mods:mods>
      )
    ).headOption.getOrElse("")
    assert(result === "74 p. ; 23 cm.")
  }

  it should "extract identifier" in {

    val result = mapping.identifier(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:recordInfo>
            <mods:descriptionStandard>aacr</mods:descriptionStandard>
            <mods:recordCreationDate encoding="marc">850516</mods:recordCreationDate>
            <mods:recordChangeDate encoding="iso8601">20060123</mods:recordChangeDate>
            <mods:recordIdentifier source="MH:ALMA">990000915260203941</mods:recordIdentifier>
            <mods:recordOrigin>Converted from MARCXML to MODS version 3.6 using MARC21slim2MODS3-6.xsl
              (Revision 1.117 2017/02/14)
            </mods:recordOrigin>
          </mods:recordInfo>
        </mods:mods>
      )
    ).headOption.getOrElse("")
    assert(result === "990000915260203941")
  }

  it should "extract language" in {
    val results = mapping.language(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:language>
            <mods:languageTerm type="code" authority="iso639-2b">spa</mods:languageTerm>
            <mods:languageTerm type="text" authority="iso639-2b">Spanish; Castilian</mods:languageTerm>
          </mods:language>
        </mods:mods>
      )
    ).map(_.providedLabel)
    assert(results.contains(Some("spa")))
    assert(results.contains(Some("Spanish; Castilian")))
  }

  it should "extract subject" in {
    val result = mapping.subject(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:subject authority="lcsh">
            <mods:name type="personal">
              <mods:namePart>Soublette, Félix</mods:namePart>
            </mods:name>
            <mods:titleInfo>
              <mods:title>Gloria de Páez</mods:title>
            </mods:titleInfo>
          </mods:subject>
        </mods:mods>
      )
    ).map(_.providedLabel.getOrElse(""))
    assert(result.contains("Soublette, Félix"))
  }

  it should "extract title" in {
    val result = mapping.title(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:titleInfo>
            <mods:title>Estudio crítico-histórico acerca del canto épico del señor Félix Soublette, La gloria de
              Páez, premiado por la Academia Venezolana
            </mods:title>
          </mods:titleInfo>
        </mods:mods>
      )
    ).headOption.getOrElse("")
    assert(result === "Estudio crítico-histórico acerca del canto épico del señor Félix Soublette, La gloria de\n              Páez, premiado por la Academia Venezolana")
  }

  it should "extract type" in {
    val result = mapping.`type`(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:typeOfResource>text</mods:typeOfResource>
        </mods:mods>
      )
    ).headOption.getOrElse("")
    assert(result === "text")
  }

  it should "extract publisher" in {
    val result = mapping.publisher(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:originInfo>
            <mods:place>
              <mods:placeTerm type="code" authority="marccountry">ve</mods:placeTerm>
              <mods:placeTerm type="text" authority="marccountry">Venezuela</mods:placeTerm>
            </mods:place>
            <mods:place>
              <mods:placeTerm type="text">Caracas</mods:placeTerm>
            </mods:place>
            <mods:publisher>Tip. Editorial de &quot;El Avisador Comercial&quot;</mods:publisher>
            <mods:dateIssued>1888</mods:dateIssued>
            <mods:issuance>monographic</mods:issuance>
          </mods:originInfo>
        </mods:mods>
      )
    ).headOption.getOrElse(EdmAgent()).name.getOrElse("")
    assert(result === "Tip. Editorial de \"El Avisador Comercial\"")
  }

  it should "extract format" in {
    val result = mapping.format(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:physicalDescription>
            <mods:form authority="marcform">print</mods:form>
            <mods:extent>74 p. ; 23 cm.</mods:extent>
          </mods:physicalDescription>
        </mods:mods>
      )
    ).headOption.getOrElse("")
    assert(result === "print")
  }

  //TODO
  it should "extract place" in {
    val result = mapping.place(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        </mods:mods>
      )
    ).headOption.getOrElse(DplaPlace()).name.getOrElse("")
    assert(result === "")
  }

  it should "extract relation" in {
    val result = mapping.relation(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:relatedItem type="series">
            <mods:titleInfo>
              <mods:title>Latin American pamphlet digital project at Harvard University</mods:title>
            </mods:titleInfo>
          </mods:relatedItem>
          <mods:relatedItem type="series">
            <mods:titleInfo>
              <mods:title>Latin American pamphlet digital project at Harvard University. 4245</mods:title>
              <mods:partName>Preservation microfilm collection</mods:partName>
            </mods:titleInfo>
          </mods:relatedItem>
        </mods:mods>
      )
    )
    assert(result.contains(Left("Latin American pamphlet digital project at Harvard University")))
    assert(result.contains(Left("Latin American pamphlet digital project at Harvard University. 4245")))
  }

  //todoi
  it should "extract collection" in {
    val result = mapping.collection(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        </mods:mods>
      )
    ).headOption.getOrElse(DcmiTypeCollection()).title.getOrElse("")
    assert(result === "")
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
