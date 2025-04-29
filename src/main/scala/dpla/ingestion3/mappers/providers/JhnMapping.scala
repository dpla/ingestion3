package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.executors.DplaMap
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.{CHProviderRegistry, Utils}
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._

/*
[/record/metadata/RDF/WebResource/rights,1813]
[/record/metadata/RDF/WebResource/isReferencedBy,6436]
[/record/metadata/RDF/WebResource/isNextInSequence,565]
[/record/metadata/RDF/WebResource/has_service,44527]
[/record/metadata/RDF/WebResource/format,6063]
[/record/metadata/RDF/WebResource/extent,6063]
[/record/metadata/RDF/WebResource/description,6063]
[/record/metadata/RDF/WebResource,17823]
[/record/metadata/RDF/Service/implements,11593]
[/record/metadata/RDF/Service/conformsTo,44527]
[/record/metadata/RDF/ProvidedCHO/type,235832]
[/record/metadata/RDF/ProvidedCHO/title,150496]
[/record/metadata/RDF/ProvidedCHO/temporal,11173]
[/record/metadata/RDF/ProvidedCHO/tableOfContents,1718]
[/record/metadata/RDF/ProvidedCHO/subject,206197]
[/record/metadata/RDF/ProvidedCHO/spatial,50364]
[/record/metadata/RDF/ProvidedCHO/source,503]
[/record/metadata/RDF/ProvidedCHO/sameAs,47296]
[/record/metadata/RDF/ProvidedCHO/rights,4224]
[/record/metadata/RDF/ProvidedCHO/replaces,9]
[/record/metadata/RDF/ProvidedCHO/relation,14512]
[/record/metadata/RDF/ProvidedCHO/publisher,7487]
[/record/metadata/RDF/ProvidedCHO/provenance,3041]
[/record/metadata/RDF/ProvidedCHO/medium,13203]
[/record/metadata/RDF/ProvidedCHO/language,41880]
[/record/metadata/RDF/ProvidedCHO/issued,3057]
[/record/metadata/RDF/ProvidedCHO/isVersionOf,28]
[/record/metadata/RDF/ProvidedCHO/isReplacedBy,17]
[/record/metadata/RDF/ProvidedCHO/isReferencedBy,1775]
[/record/metadata/RDF/ProvidedCHO/isPartOf,58186]
[/record/metadata/RDF/ProvidedCHO/identifier,39522]
[/record/metadata/RDF/ProvidedCHO/hasPart,8]
[/record/metadata/RDF/ProvidedCHO/hasFormat,1419]
[/record/metadata/RDF/ProvidedCHO/format,76627]
[/record/metadata/RDF/ProvidedCHO/extent,1776]
[/record/metadata/RDF/ProvidedCHO/description,319257]
[/record/metadata/RDF/ProvidedCHO/date,77239]
[/record/metadata/RDF/ProvidedCHO/creator,19425]
[/record/metadata/RDF/ProvidedCHO/created,119]
[/record/metadata/RDF/ProvidedCHO/coverage,10963]
[/record/metadata/RDF/ProvidedCHO/contributor,26011]
[/record/metadata/RDF/ProvidedCHO/alternative,6306]
[/record/metadata/RDF/Place/sameAs,17798]
[/record/metadata/RDF/Place/prefLabel,17244]
[/record/metadata/RDF/Place/note,339]
[/record/metadata/RDF/Place/long,1348]
[/record/metadata/RDF/Place/lat,1348]
[/record/metadata/RDF/Place/altLabel,167529]
[/record/metadata/RDF/Concept/prefLabel,6021]
[/record/metadata/RDF/Concept/note,6021]
[/record/metadata/RDF/Concept/altLabel,6021]
[/record/metadata/RDF/Aggregation/rights,70857]
[/record/metadata/RDF/Aggregation/provider,70857]
[/record/metadata/RDF/Aggregation/object,20925]
[/record/metadata/RDF/Aggregation/isShownBy,52929]
[/record/metadata/RDF/Aggregation/isShownAt,31945]
[/record/metadata/RDF/Aggregation/hasView,568]
[/record/metadata/RDF/Aggregation/dataProvider,70857]
[/record/metadata/RDF/Aggregation/aggregatedCHO,70857]
[/record/metadata/RDF/Agent/sameAs,38038]
[/record/metadata/RDF/Agent/prefLabel,14281]
[/record/metadata/RDF/Agent/placeOfDeath,973]
[/record/metadata/RDF/Agent/placeOfBirth,1023]
[/record/metadata/RDF/Agent/note,2589]
[/record/metadata/RDF/Agent/identifier,2131]
[/record/metadata/RDF/Agent/dateOfDeath,12853]
[/record/metadata/RDF/Agent/dateOfBirth,12847]
[/record/metadata/RDF/Agent/biographicalInformation,22]
[/record/metadata/RDF/Agent/altLabel,27870]
[/record/header/setSpec,70857]
[/record/header/identifier,70857]
[/record/header/datestamp,70857] */

class JhnMapping
    extends XmlMapping
    with XmlExtractor
    with IngestMessageTemplates {

  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("jhn")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(metadataRoot(data) \ "Aggregation" \ "dataProvider")
      .map(nameOnlyAgent)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    (metadataRoot(data) \ "Aggregation" \ "rights")
      .flatMap(node => getAttributeValue(node, "rdf:resource"))
      .map(URI)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    (metadataRoot(data) \ "Aggregation" \ "isShownAt")
      .flatMap(node => getAttributeValue(node, "rdf:resource"))
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  // SourceResource
  override def collection(
      data: Document[NodeSeq]
  ): ZeroToMany[DcmiTypeCollection] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "isPartOf")
      .map(nameOnlyCollection)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    val agents = (metadataRoot(data) \ "Agent").map(agent => {
      // TODO rdaGr2:dateOfBirth and rdaGr2:dateOfDeath...?
      val name = extractString(agent \ "prefLabel")
      val note = extractString(agent \ "note")
      val uri = getAttributeValue(agent, "rdf:about").map(URI)
      EdmAgent(
        name = name,
        note = note,
        uri = uri
      )
    })

    val creators =
      extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "creator")
        .map(nameOnlyAgent)

    // Similar values exist in both dc:creators and edm:Agent, prefer to use edm:Agent
    if (agents.nonEmpty)
      agents
    else
      creators
  }

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "contributor")
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "date")
      .map(stringOnlyTimeSpan)

  // TODO filter by language...?
  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "description")

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "format")

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "identifier")

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "language")
      .map(nameOnlyConcept)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    (metadataRoot(data) \ "Aggregation" \ "isShownBy")
      .flatMap(node => getAttributeValue(node, "rdf:resource"))
      .map(stringOnlyWebResource)

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] = {
    val placeConcepts = (metadataRoot(data) \ "Place").map(node => {
      val prefLabel = extractString(node \ "prefLabel")
      val coordinates =
        (extractString(node \ "lat"), extractString(node \ "long")) match {
          case (Some(lat: String), Some(long: String)) => Some(s"$lat,$long")
          case _                                       => None
        }
      DplaPlace(
        name = prefLabel,
        coordinates = coordinates
      )
    })

    val spatial = extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "spatial")
      .map(nameOnlyPlace)

    // Similar values can exist in both edm:Place and dc:spatial, use edm:Place if defined otherwise dc:spatial
    if (placeConcepts.nonEmpty)
      placeConcepts
    else
      spatial
  }

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("Jewish Heritage Network"),
    uri = Some(URI("http://dp.la/api/contributor/jhn"))
  )

  // Concept
  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    val concepts = (metadataRoot(data) \ "Concept").map(node => {
      val prefLabel = extractString(node \ "prefLabel")
      val note = extractString(node \ "note")
      val uri = getAttributeValue(node, "rdf:about").toSeq.map(URI)

      SkosConcept(
        providedLabel = prefLabel,
        note = note,
        exactMatch = uri
      )
    })

    val subjects = extractStrings(
      metadataRoot(data) \ "ProvidedCHO" \ "subject"
    ).map(nameOnlyConcept)
    // Similar values exist in skos:Concept and dc:subject, prefer skos:Concept over dc:subject
    if (concepts.nonEmpty)
      concepts
    else
      subjects
  }

  // TODO filter by language..?
  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "title")

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "type")
      .flatMap(_.splitAtDelimiter(";"))

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(
      data
    ))

  private def metadataRoot(data: Document[NodeSeq]): NodeSeq =
    data \ "metadata" \ "RDF"
}

object JhnMapping extends JhnMapping {
  def main(args: Array[String]): Unit = {

    val data = Document(record)

    val dplaMap = new DplaMap()
    val extractorClass = CHProviderRegistry.lookupProfile("jhn").get
    val oreAggregation = dplaMap.map(record.toString(), extractorClass)
    println(oreAggregation.isShownAt.uri)

  }



  val record = <record xmlns="http://www.openarchives.org/OAI/2.0/"
                       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <header>
      <identifier>oai:jts:jts_489795</identifier>
      <datestamp>2022-08-20T19:57:48Z</datestamp>
      <setSpec>JTSA</setSpec>
    </header>
    <metadata>
      <rdf:RDF xmlns:rdaGr2="http://rdvocab.info/ElementsGr2/"
               xmlns:wgs84_pos="http://www.w3.org/2003/01/geo/wgs84_pos#"
               xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
               xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/"
               xmlns:dct="http://purl.org/dc/terms/" xmlns:edm="http://www.europeana.eu/schemas/edm/"
               xmlns:foaf="http://xmlns.com/foaf/0.1/" xmlns:owl="http://www.w3.org/2002/07/owl#"
               xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
               xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
               xmlns:skos="http://www.w3.org/2004/02/skos/core#"
               xmlns:ore="http://www.openarchives.org/ore/terms/"
               xmlns:svcs="http://rdfs.org/sioc/services#" xmlns:doap="http://usefulinc.com/ns/doap#">
        <edm:Agent rdf:about="https://data.jhn.ngo/persons/JTSA/5297">
          <skos:prefLabel>Jewish Theological Seminary Library</skos:prefLabel>
          <rdaGr2:dateOfBirth>8092</rdaGr2:dateOfBirth>
          <rdaGr2:dateOfDeath>-</rdaGr2:dateOfDeath>
        </edm:Agent>
        <edm:ProvidedCHO
        rdf:about="https://digitalcollections.jtsa.edu/islandora/object/jts%3A489795/datastream/TN/view/Manuscripts%3A%20V%CC%A3a-yik%CC%A3ra%2C%20Be-midbar%20u-Devarim%2C%20%CA%BBim%20Targum%20Onk%CC%A3elos%20v%CC%A3e-Tafsir%20Rasag%2C%20be-liv%CC%A3ui%20perush%20Rashi.jpg">
          <dc:contributor> Jewish Theological Seminary Library. Manuscript. 10502. </dc:contributor>
          <dc:coverage>Yemen</dc:coverage>
          <dc:date>1800-1899</dc:date>
          <dc:date>[18--]</dc:date>
          <dc:description> Copy imperfect: ff. 1-3, 84, and all after 220 wanting </dc:description>
          <dc:description>Includes Leviticus 3:8 to Deuteronomy 33:7</dc:description>
          <dc:description> Cataloging and digitization funded by an anonymous challenge grant,
            2014-2015. </dc:description>
          <dc:description> Shelfmark: New York, The Library of The Jewish Theological
            Seminary, MS 10502. </dc:description>
          <dc:description>Katsh, Salem Gift 1998 July.</dc:description>
          <dc:description> Hebrew, Aramaic and Judeo-Arabic : Yemenite square and semi-cursive
            hands. </dc:description>
          <dc:description>Katsh, Abraham I., collection.</dc:description>
          <dc:description>surveyed 9807 SL.</dc:description>
          <dc:description>digitized 20150706 UM pda NNJ DP</dc:description>
          <dc:description xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                          edm:confidenceLevel="0.75"> העתק לא מושלם: ff. 1-3, 84, וכל אחרי 220 רוצה</dc:description>
          <dc:description xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                          edm:confidenceLevel="0.75"> כולל ויקרא 3:8 לדברות 33:7</dc:description>
          <dc:description xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                          edm:confidenceLevel="0.75"> קיטלוג ודיגיטציה במימון מענק אתגר אנונימי,
            2015-2014.</dc:description>
          <dc:description xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                          edm:confidenceLevel="0.75"> סימן מדף: ניו יורק, ספריית הסמינר התיאולוגי היהודי,
            MS 10502.</dc:description>
          <dc:description xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                          edm:confidenceLevel="0.75"> קאטש, סאלם מתנה 1998 יולי.</dc:description>
          <dc:description xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                          edm:confidenceLevel="0.75"> עברית, ארמית ויהודית-ערבית : כיכר תימנית וידיים חצי
            טחונות.</dc:description>
          <dc:description xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                          edm:confidenceLevel="0.75"> קאטש, אברהם א., אוסף.</dc:description>
          <dc:description xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                          edm:confidenceLevel="0.75"> סקר 9807 SL.</dc:description>
          <dc:description xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                          edm:confidenceLevel="0.75"> דיגיטציה 20150706 UM פדה NNJ DP</dc:description>
          <dc:format>220 [i.e. 216] leaves ; 23 x 19.5 cm.</dc:format>
          <dc:identifier>MS 10502</dc:identifier>
          <dc:identifier>SHF1772:20</dc:identifier>
          <dc:identifier>jts:489795</dc:identifier>
          <dc:language>heb</dc:language>
          <dc:language>arc</dc:language>
          <dc:language>jrb</dc:language>
          <dc:relation>Perush Rashi ʻal ha-Torah. 1800</dc:relation>
          <dc:subject />
          <dc:title> ויקרא, במדבר ודברים, עם תרגום אונקלוס ותפסיר רס&quot;ג, בליווי פירוש
            רש&quot;י </dc:title>
          <dc:title> Ṿa-yiḳra, Be-midbar u-Devarim, ʻim Targum Onḳelos ṿe-Tafsir Rasag,
            be-liṿui perush Rashi </dc:title>
          <dc:title>Perush Rashi ʻal ha-Torah</dc:title>
          <dc:title>Bible. Pentateuch. Hebrew. 1800</dc:title>
          <dc:title>Bible. Onḳelos. Pentateuch. Aramaic. 1800</dc:title>
          <dc:title>Bible. Saadia. Pentateuch. Judeo-Arabic. 1800</dc:title>
          <dc:title xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                    edm:confidenceLevel="0.75"> ויקרא, במדבר ודברים, עם תרגום אונקלוס ותפסיר
            רס&quot;ג, בליווי פירוש רש&quot;י</dc:title>
          <dc:title xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                    edm:confidenceLevel="0.75"> Ṿa-yiḳra, בית-מידבר יו-דברים, ים טרגום Onḳelos
            ṿe-טפסיר רסאג, liṿui פרוש רש&quot;י</dc:title>
          <dc:title xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                    edm:confidenceLevel="0.75"> פרוש רש&quot;י על התורה</dc:title>
          <dc:title xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                    edm:confidenceLevel="0.75"> ביבליה. פינטאטוש ( Pentateuch ). עברית. 1800</dc:title>
          <dc:title xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                    edm:confidenceLevel="0.75"> ביבליה. Onḳelos. פינטאטוש ( Pentateuch ). ארמית.
            1800</dc:title>
          <dc:title xml:lang="he" edm:wasGeneratedBy="SoftwareAgent"
                    edm:confidenceLevel="0.75"> ביבליה. סעדיה, סעדיה. פינטאטוש ( Pentateuch ). ערבית
            יהודית. 1800</dc:title>
          <dc:type>Text</dc:type>
          <dc:type>Manuscripts.</dc:type>
          <dcterms:isPartOf xml:lang="en"> Europeana XX: Century of Change </dcterms:isPartOf>
          <edm:type>TEXT</edm:type>
        </edm:ProvidedCHO>
        <edm:WebResource rdf:about="MS10502" />
        <ore:Aggregation rdf:about="MS10502#aggregation">
          <edm:aggregatedCHO
          rdf:resource="https://digitalcollections.jtsa.edu/islandora/object/jts%3A489795/datastream/TN/view/Manuscripts%3A%20V%CC%A3a-yik%CC%A3ra%2C%20Be-midbar%20u-Devarim%2C%20%CA%BBim%20Targum%20Onk%CC%A3elos%20v%CC%A3e-Tafsir%20Rasag%2C%20be-liv%CC%A3ui%20perush%20Rashi.jpg#agg"></edm:aggregatedCHO>
          <edm:dataProvider>Library of The Jewish Theological Seminary</edm:dataProvider>
          <edm:isShownAt
          rdf:resource="https://digitalcollections.jtsa.edu/islandora/object/jts%3A489795"></edm:isShownAt>
          <edm:object
          rdf:resource="https://digitalcollections.jtsa.edu/islandora/object/jts%3A489795/datastream/TN/view/Manuscripts%3A%20V%CC%A3a-yik%CC%A3ra%2C%20Be-midbar%20u-Devarim%2C%20%CA%BBim%20Targum%20Onk%CC%A3elos%20v%CC%A3e-Tafsir%20Rasag%2C%20be-liv%CC%A3ui%20perush%20Rashi.jpg"></edm:object>
          <edm:provider>Judaica Europeana/Jewish Heritage Network</edm:provider>
          <edm:rights rdf:resource="http://rightsstatements.org/vocab/InC-EDU/1.0/" />
        </ore:Aggregation>
      </rdf:RDF>
    </metadata>
  </record>
}