package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class JhnMapping extends XmlMapping with XmlExtractor
  with IngestMessageTemplates {

  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("jhn")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(metadataRoot(data) \ "Aggregation" \ "dataProvider")
      .map(nameOnlyAgent)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    (metadataRoot(data) \ "Aggregation" \ "rights")
      .flatMap(node => getAttributeValue(node, s"rdf:resource"))
      .map(URI)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    (metadataRoot(data) \ "Aggregation" \ "isShownAt")
      .flatMap(node => getAttributeValue(node, "rdf:resource"))
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  // SourceResource
  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] = 
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "isPartOf").map(nameOnlyCollection)

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

    val creators = extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "creator")
      .map(nameOnlyAgent)

    // Similar values exist in both dc:creators and edm:Agent, prefer to use edm:Agent
    if(agents.nonEmpty)
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
      val lat = extractString(node \ "lat")
      val long = extractString(node \ "long")
      DplaPlace(
        name = prefLabel,
        coordinates = Some(s"$lat,$long")
      )
    })

    val spatial = extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "spatial")
      .map(nameOnlyPlace)

    // Similar values can exist in both edm:Place and dc:spatial, use edm:Place if defined otherwise dc:spatial
    if(placeConcepts.nonEmpty)
      placeConcepts
    else
      spatial
  }

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  // Concept
  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    val concepts = (metadataRoot(data) \ "Concept").map(node => {
      val prefLabel = extractString(node \ "prefLabel")
      val note = extractString(node \ "note")
      val altLabel = extractString(node \ "altLabel")
      val uri = getAttributeValue(node, "rdf:about").toSeq.map(URI)

      SkosConcept(
        concept = prefLabel,
        providedLabel = altLabel,
        note = note,
        exactMatch = uri
      )
    })

    val subjects = extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "subject").map(nameOnlyConcept)
    // Similar values exist in skos:Concept and dc:subject, prefer skos:Concept over dc:subject
    if(concepts.nonEmpty)
      concepts
    else
      subjects
  }

  // TODO filter by language..?
  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(metadataRoot(data ) \ "ProvidedCHO" \ "title")

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data ) \ "ProvidedCHO" \ "type")
      .flatMap(_.splitAtDelimiter(";"))

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  private def agent = EdmAgent(
    name = Some("Jewish Heritage Network"),
    uri = Some(URI("http://dp.la/api/contributor/jhn"))
  )

  private def metadataRoot(data: Document[NodeSeq]): NodeSeq = data \ "metadata" \ "RDF"
}
