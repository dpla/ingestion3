package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml._

class HarvardMappingTest extends AnyFlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] =
    new MessageCollector[IngestMessage]

  val mapping = new HarvardMapping

  "A Harvard mapping" should "have the correct provider name" in {
    assert(mapping.getProviderName === Some("harvard"))
  }

  it should "extract the correct alternative title from MARC-derived records" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:mods="http://www.loc.gov/mods/v3"
                   xmlns:sets="http://hul.harvard.edu/ois/xml/ns/sets"
                   xmlns:xlink="http://www.w3.org/1999/xlink"
                   xmlns:marc="http://www.loc.gov/MARC21/slim"
                   xmlns:HarvardDRS="http://hul.harvard.edu/ois/xml/ns/HarvardDRS"
                   xmlns:librarycloud="http://hul.harvard.edu/ois/xml/ns/librarycloud"
                   xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-6.xsd"
                   version="3.6">
          <mods:titleInfo>
            <mods:title>Cryptogamic botany</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Alaska</mods:title>
            <mods:partNumber>Volume V</mods:partNumber>
            <mods:partName>Cryptogamic botany</mods:partName>
          </mods:titleInfo>
        </mods:mods>
      )
    )
    val result = mapping.alternateTitle(document)
    assert(result === Seq("Alaska Volume V Cryptogamic botany"))
  }

  it should "extract the correct alternative title from image metadata" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:mods="http://www.loc.gov/mods/v3"
                   xmlns:sets="http://hul.harvard.edu/ois/xml/ns/sets"
                   xmlns:xlink="http://www.w3.org/1999/xlink"
                   xmlns:marc="http://www.loc.gov/MARC21/slim"
                   xmlns:HarvardDRS="http://hul.harvard.edu/ois/xml/ns/HarvardDRS"
                   xmlns:librarycloud="http://hul.harvard.edu/ois/xml/ns/librarycloud"
                   xsi:schemaLocation="http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-6.xsd"
                   version="3.6">
          <mods:titleInfo>
            <mods:title>Stele of "Chunhua ge fa tie", (Model-Letter Compendia of the Chunhua reign)-- 8th. volume: 3rd. volume of Wang Xizhi's calligraphy models</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Ge tie</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Guan tie</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Chunhua ge tie</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Chun hua fa tie</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Chunhua ge mi ge tie</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Chunhua mi ge fa tie</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Su wang fu ben Chunhua ge fa tie</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Lanzhou Chunhua ge tie</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Ming ta su fu ben</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Xi'an ben</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Guan zhong ben</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Guan fa tie</mods:title>
          </mods:titleInfo>
          <mods:titleInfo type="alternative">
            <mods:title>Chunhua ge fa tie di ba (Wang Xizhi)</mods:title>
          </mods:titleInfo>
        </mods:mods>
      )
    )
    val result = mapping.alternateTitle(document)
    assert(
      result.sorted === Seq(
        "Ge tie",
        "Guan tie",
        "Chunhua ge tie",
        "Chun hua fa tie",
        "Chunhua ge mi ge tie",
        "Chunhua mi ge fa tie",
        "Su wang fu ben Chunhua ge fa tie",
        "Lanzhou Chunhua ge tie",
        "Ming ta su fu ben",
        "Xi'an ben",
        "Guan zhong ben",
        "Guan fa tie",
        "Chunhua ge fa tie di ba (Wang Xizhi)"
      ).sorted
    )
  }

  it should "get the correct original ID" in {
    val document = record(
      header = header(<identifier>990000915260203941</identifier>)
    )
    val result = mapping.originalId(document)
    assert(result === Some("990000915260203941"))
  }

  it should "create the correct DPLA URI" in {
    val document = record(
      header = header(
        <identifier>990000915260203941</identifier>
      )
    )
    val result = mapping.dplaUri(document)
    val expected =
      Some(URI("http://dp.la/api/items/7fd6f05e2d23801cb1bff577e6df5f14"))
    assert(result === expected)
  }

  it should "return the correct data provider" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:location>
            <mods:physicalLocation type="repository" displayLabel="Harvard repository">
              Master Microforms, Harvard University
            </mods:physicalLocation>
          </mods:location>
        </mods:mods>
      )
    )
    val result = mapping
      .dataProvider(document)
      .headOption
      .getOrElse(EdmAgent())
      .name

    assert(result === Some("Master Microforms, Harvard University"))
  }

  it should "return the correct rights statement" in {
    // Hardcoded right now
    val document = record(metadata = metadata(<modsorsomething/>))
    val result = mapping.rights(document)
    assert(result === Seq("Held in the collections of Harvard University."))
  }

  it should "return the correct isShownAt" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:extension>
            <sets:sets>
              <sets:set>
                <sets:setName>Latin American Pamphlet Digital Collection</sets:setName>
                <sets:setSpec>latin</sets:setSpec>
              </sets:set>
              <sets:set>
                <sets:systemId>10</sets:systemId>
                <sets:setName>Scanned Maps</sets:setName>
                <sets:setSpec>maps</sets:setSpec>
                <sets:baseUrl>https://id.lib.harvard.edu/curiosity/scanned-maps/44-</sets:baseUrl>
              </sets:set>
            </sets:sets>
          </mods:extension>
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
      ),
      about =
        about(<request verb="ListRecords" metadataPrefix="mods" set="latin"/>)
    )

    val results = mapping
      .isShownAt(document)
      .map(_.uri)

    // Harvard does not want Harvard Digital Collections links
    assert(
      !results.contains(
        URI("https://id.lib.harvard.edu/digital_collections/990000915260203941")
      )
    )

    assert(
      results.contains(
        URI(
          "https://id.lib.harvard.edu/curiosity/latin-american-pamphlet-digital-collection/43-990000915260203941"
        )
      )
    )
  }

  it should "return the originalRecord" in {
    val result = mapping.originalRecord(record())
    assert(result.contains("<record"))
  }

  it should "return the preview" in {
    val document = record(
      metadata = metadata(
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
    )
    val result = mapping
      .preview(document)
      .headOption
      .getOrElse(EdmWebResource(uri = URI("")))
      .uri
    assert(
      result === URI(
        "https://ids.lib.harvard.edu/ids/view/4965202?width=150&height=150&usethumb=y"
      )
    )
  }

  it should "look for previews in mods:relatedItem" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:location>
            <mods:url access="raw object" note="Provides access to page images of entire work">
              https://nrs.harvard.edu/urn-3:FHCL:923450
            </mods:url>
            <mods:url access="object in context" displayLabel="Harvard Digital Collections">
              https://id.lib.harvard.edu/digital_collections/990000915260203941
            </mods:url>
            <mods:url access="object in context" displayLabel="Latin American Pamphlet Digital Collection">
              https://id.lib.harvard.edu/curiosity/latin-american-pamphlet-digital-collection/43-990000915260203941
            </mods:url>
          </mods:location>
          <mods:relatedItem type="constituent">
            <mods:location>
              <mods:url access="preview">https://ids.lib.harvard.edu/ids/view/4965202?width=150&amp;height=150&amp;usethumb=y</mods:url>
              <mods:url access="fofo">https://example.com</mods:url>
            </mods:location>
          </mods:relatedItem>
        </mods:mods>
      )
    )
    val results = mapping.preview(document)
    assert(results.size === 1)
    assert(
      results.headOption
        .getOrElse(stringOnlyWebResource(""))
        .uri
        .toString === "https://ids.lib.harvard.edu/ids/view/4965202?width=150&height=150&usethumb=y"
    )
  }

  it should "return the provider" in {
    val expected = EdmAgent(
      name = Some("Harvard Library"),
      uri = Some(URI("http://dp.la/api/contributor/harvard"))
    )
    val result = mapping.provider(record())
    assert(result === expected)
  }

  it should "extract a contributor" in {
    val document = record(
      metadata = metadata(
        <mods:mods>
          <mods:name type="personal">
            <mods:namePart>Garman, Samuel</mods:namePart>
            <mods:namePart type="date">1843-1927</mods:namePart>
            <mods:role>
              <mods:roleTerm type="text">creator</mods:roleTerm>
            </mods:role>
          </mods:name>
          <mods:name type="personal">
            <mods:namePart>Agassiz, Alexander</mods:namePart>
            <mods:namePart type="date">1835-1910</mods:namePart>
          </mods:name>
          <mods:name type="corporate">
            <mods:namePart>Albatross (Steamer)</mods:namePart>
          </mods:name>
          <mods:name type="corporate">
            <mods:namePart>Harvard University</mods:namePart>
            <mods:namePart>Museum of Comparative Zoology.</mods:namePart>
          </mods:name>
        </mods:mods>
      )
    )
    val result = mapping
      .contributor(document)
      .flatMap(_.name)

    assert(
      result === Seq(
        "Agassiz, Alexander, 1835-1910",
        "Albatross (Steamer)",
        "Harvard University Museum of Comparative Zoology."
      )
    )
  }

  it should "extract a creator" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:name type="personal">
            <mods:namePart>Garman, Samuel</mods:namePart>
            <mods:namePart type="date">1843-1927</mods:namePart>
            <mods:role>
              <mods:roleTerm type="text">creator</mods:roleTerm>
            </mods:role>
          </mods:name>
          <mods:name type="personal">
            <mods:namePart>Agassiz, Alexander</mods:namePart>
            <mods:namePart type="date">1835-1910</mods:namePart>
          </mods:name>
          <mods:name type="corporate">
            <mods:namePart>Albatross (Steamer)</mods:namePart>
          </mods:name>
          <mods:name type="corporate">
            <mods:namePart>Harvard University</mods:namePart>
            <mods:namePart>Museum of Comparative Zoology.</mods:namePart>
          </mods:name>
        </mods:mods>
      )
    )
    val result = mapping
      .creator(document)
      .headOption
      .getOrElse(EdmAgent())
      .name
    assert(result === Some("Garman, Samuel, 1843-1927"))
  }

  it should "extract dates" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:originInfo>
            <mods:place>
              <mods:placeTerm type="code" authority="marccountry">ve</mods:placeTerm>
              <mods:placeTerm type="text" authority="marccountry">Venezuela</mods:placeTerm>
            </mods:place>
            <mods:place>
              <mods:placeTerm type="text">Caracas</mods:placeTerm>
            </mods:place>
            <mods:publisher>Tip. Editorial de
              &quot;
              El Avisador Comercial
              &quot;
            </mods:publisher>
            <mods:dateIssued>1888</mods:dateIssued>
            <mods:issuance>monographic</mods:issuance>
          </mods:originInfo>
        </mods:mods>
      )
    )
    val result = mapping
      .date(document)
      .headOption
      .getOrElse(EdmTimeSpan())
      .originalSourceDate
    assert(result === Some("1888"))
  }

  it should "extract descriptions" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:abstract>foo</mods:abstract>
          <mods:note>bar</mods:note>
        </mods:mods>
      )
    )
    val result = mapping.description(document)
    assert(result === Seq("foo", "bar"))
  }

  it should "extract extent" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:physicalDescription>
            <mods:form authority="marcform">print</mods:form>
            <mods:extent>74 p. ; 23 cm.</mods:extent>
          </mods:physicalDescription>
        </mods:mods>
      )
    )
    val result = mapping
      .extent(document)
      .headOption
      .getOrElse("")
    assert(result === "74 p. ; 23 cm.")
  }

  it should "extract identifier" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:identifier>foo</mods:identifier>
          <mods:recordInfo>
            <mods:descriptionStandard>aacr</mods:descriptionStandard>
            <mods:recordCreationDate encoding="marc">850516</mods:recordCreationDate>
            <mods:recordChangeDate encoding="iso8601">20060123</mods:recordChangeDate>
            <mods:recordIdentifier source="MH:VIA">990000915260203941</mods:recordIdentifier>
            <mods:recordOrigin>Converted from MARCXML to MODS version 3.6 using MARC21slim2MODS3-6.xsl
              (Revision 1.117 2017/02/14)
            </mods:recordOrigin>
          </mods:recordInfo>
        </mods:mods>
      )
    )
    val result = mapping.identifier(document)
    assert(result === Seq("990000915260203941", "foo"))
  }

  it should "extract language" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:language>
            <mods:languageTerm type="code" authority="iso639-2b">spa</mods:languageTerm>
            <mods:languageTerm type="text" authority="iso639-2b">Spanish; Castilian</mods:languageTerm>
          </mods:language>
        </mods:mods>
      )
    )
    val results = mapping.language(document)
    assert(
      results === Seq(SkosConcept(providedLabel = Some("Spanish; Castilian")))
    )
  }

  it should "extract subject" in {
    val document = record(
      metadata = metadata(
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
    )
    val result = mapping
      .subject(document)
      .map(_.providedLabel.getOrElse(""))
    assert(result.contains("Soublette, Félix"))
    assert(result.contains("Gloria de Páez"))
  }

  it should "extract title" in {
    val document = record(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:titleInfo>
            <mods:title>Estudio crítico-histórico acerca del canto épico del señor Félix Soublette, La gloria de
              Páez, premiado por la Academia Venezolana
            </mods:title>
          </mods:titleInfo>
        </mods:mods>
      )
    )
    val result = mapping
      .title(document)
      .headOption
      .getOrElse("")
    assert(
      result === "Estudio crítico-histórico acerca del canto épico del señor Félix Soublette, La gloria de\n              Páez, premiado por la Academia Venezolana"
    )
  }

  it should "extract title and collection title" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:titleInfo>
            <mods:title>Title</mods:title>
          </mods:titleInfo>
          <mods:relatedItem type="host" displayLabel="collection">
            <mods:location>
              <mods:physicalLocation valueURI="http://isni.org/isni/0000000121705139" displayLabel="Harvard repository" type="repository">
                Andover-Harvard Theological Library, Harvard Divinity School, Harvard University</mods:physicalLocation>
            </mods:location>
            <mods:identifier>bMS 575</mods:identifier>
            <mods:titleInfo>
              <mods:title>Foote, Henry Wilder, 1875-1964. Papers of Professor Henry Wilder Foote and Family, 1714-1959.</mods:title>
            </mods:titleInfo>
            <mods:originInfo>
              <mods:dateCreated point="start">1714</mods:dateCreated>
              <mods:dateCreated point="end">1959</mods:dateCreated>
              <mods:dateCreated>1714-1959</mods:dateCreated>
            </mods:originInfo>
            <mods:recordInfo>
              <mods:recordIdentifier>div00575</mods:recordIdentifier>
            </mods:recordInfo>
            <mods:relatedItem otherType="HOLLIS record">
              <mods:location>
                <mods:url>https://id.lib.harvard.edu/alma/990073094440203941/catalog</mods:url>
              </mods:location>
            </mods:relatedItem>
            <mods:relatedItem otherType="Finding Aid">
              <mods:location>
                <mods:url>https://id.lib.harvard.edu/ead/div00575/catalog</mods:url>
              </mods:location>
            </mods:relatedItem>
            <mods:extension>
              <librarycloud:HarvardRepositories>
                <librarycloud:HarvardRepository>
                  Andover-Harv. Theol
                </librarycloud:HarvardRepository>
              </librarycloud:HarvardRepositories>
            </mods:extension>
          </mods:relatedItem>
        </mods:mods>
      )
    )
    val result = mapping.title(document)
    val expected = Seq(
      "Title",
      "Foote, Henry Wilder, 1875-1964. Papers of Professor Henry Wilder Foote and Family, 1714-1959."
    )
    assert(result === expected)
  }

  it should "extract type" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:typeOfResource>text</mods:typeOfResource>
        </mods:mods>
      )
    )
    val result = mapping
      .`type`(document)
      .headOption
      .getOrElse("")
    assert(result === "text")
  }

  it should "extract type from digitalFormats" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
        <mods:extension>
          <librarycloud>
            <digitalFormats>
              <digitalFormat>Books and documents</digitalFormat>
            </digitalFormats>
          </librarycloud>
        </mods:extension>
      </mods:mods>
      )
    )
    val result = mapping.`type`(document)
    assert(result === Seq("Books and documents"))
  }

  it should "extract publisher" in {
    val document = record(
      metadata = metadata(
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
    )
    val result = mapping
      .publisher(document)
      .headOption
      .getOrElse(EdmAgent())
      .name
      .getOrElse("")
    assert(result === "Tip. Editorial de \"El Avisador Comercial\"")
  }

  it should "extract format" in {
    val document = record(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:genre>foo</mods:genre>
        </mods:mods>
      )
    )
    val result = mapping
      .format(document)
      .headOption
      .getOrElse("")
    assert(result === "foo")
  }

  it should "extract place" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:subject authority="lcsh">
            <mods:topic>Deep-sea fishes</mods:topic>
            <mods:geographic>Mexico</mods:geographic>
          </mods:subject>
          <mods:subject authority="lcsh">
            <mods:topic>Deep-sea fishes</mods:topic>
            <mods:geographic>Central America</mods:geographic>
          </mods:subject>
          <mods:subject authority="lcsh">
            <mods:topic>Deep-sea fishes</mods:topic>
            <mods:geographic>South America</mods:geographic>
          </mods:subject>
        </mods:mods>
      )
    )
    val result = mapping.place(document)
    assert(result.contains(nameOnlyPlace("South America")))
    assert(result.contains(nameOnlyPlace("Central America")))
    assert(result.contains(nameOnlyPlace("Mexico")))
  }

  it should "extract relation" in {
    val document = record(
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
    val result = mapping.relation(document)
    assert(
      result.contains(
        LiteralOrUri(
          "Latin American pamphlet digital project at Harvard University",
          isUri = false
        )
      )
    )
    assert(
      result.contains(
        LiteralOrUri(
          "Latin American pamphlet digital project at Harvard University. 4245 Preservation microfilm collection",
          isUri = false
        )
      )
    )
  }

  it should "extract collection" in {
    val document = record(
      metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:extension>
            <sets:sets>
              <sets:set>
                <sets:systemId>57207</sets:systemId>
                <sets:setName>Chinese Rubbings Collection</sets:setName>
                <sets:setSpec>rubbings</sets:setSpec>
                <sets:baseUrl>https://id.lib.harvard.edu/curiosity/chinese-rubbings-collection/6-</sets:baseUrl>
              </sets:set>
            </sets:sets>
          </mods:extension>
        </mods:mods>
      )
    )
    val result = mapping
      .collection(document)
      .headOption
      .getOrElse(DcmiTypeCollection())
      .title
      .getOrElse("")
    assert(result === "Chinese Rubbings Collection")
  }

  it should "extract temporal" in {
    val document = record(
      metadata = metadata(
        <mods:mods xmlns:mods="http://www.loc.gov/mods/v3">
          <mods:subject>
            <mods:temporal>now</mods:temporal>
          </mods:subject>
        </mods:mods>
      )
    )
    val result = mapping.temporal(document)
    assert(result.contains(stringOnlyTimeSpan("now")))
  }

  def header(headerBody: NodeSeq): NodeSeq = <header>{headerBody}</header>

  def metadata(metadataBody: NodeSeq): NodeSeq = <metadata>{
    metadataBody
  }</metadata>

  def about(aboutBody: NodeSeq): NodeSeq = <about>{aboutBody}</about>

  def record(
      header: NodeSeq = Seq(),
      metadata: NodeSeq = Seq(),
      about: NodeSeq = Seq()
  ): Document[NodeSeq] =
    Document(
      <record
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xmlns="http://www.openarchives.org/OAI/2.0/">
        {header}
        {metadata}
        {about}
      </record>
    )
}
