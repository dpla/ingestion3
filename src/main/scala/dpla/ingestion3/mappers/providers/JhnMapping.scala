package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.DigitalSurrogateBlockList
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

  //  <edm:rights rdf:resource="http://rightsstatements.org/vocab/InC/1.0/"/>
  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    (metadataRoot(data) \ "Aggregation" \ "rights")
      .map(node => getAttributeValue(node, s"rdf:resource"))
      .map(URI)

  //  override def intermediateProvider(data: Document[NodeSeq]): ZeroToOne[EdmAgent] =
  //    extractString(data \ "metadata" \ "qualifieddc" \ "mediator")
  //      .map(nameOnlyAgent)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    (metadataRoot(data) \ "Aggregation" \ "isShownAt")
      .map(node => getAttributeValue(node, "rdf:resource"))
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  // SourceResource
  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    (metadataRoot(data) \ "Agent").map(agent => {
      val name = extractString(agent \ "prefLabel")
      val note = extractString(agent \ "note")

      EdmAgent(
        name = edmAgentNameHelper(name, note),
        note = note
      )
    })
  }

  private def edmAgentNameHelper(name: Option[String], role: Option[String]): Option[String] = {
    (name, role) match {
      case (Some(n), Some(r)) => Some(s"$n, $r")
      case (Some(n), None) => Some(n)
      case (None, _) => None
      case (_, _) => None
    }
  }

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "date")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "description")

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "ProvidedCHO" \ "format")

  //  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
  //    extractStrings(data \ "metadata" \ "qualifieddc" \ "language")
  //      .flatMap(_.splitAtDelimiter(";"))
  //      .map(nameOnlyConcept)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "metadata" \ "qualifieddc" \ "source")
      .map(stringOnlyWebResource)

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    (metadataRoot(data) \ "Place").map(node => {
      val prefLabel = extractString(node \ "prefLabel")
      val lat = extractString(node \ "lat")
      val long = extractString(node \ "long")
      DplaPlace(
        name = prefLabel,
        coordinates = Some(s"$lat,$long")
      )
    })

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  // Concept
  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    (metadataRoot(data) \ "Concept").map(node => {
      val prefLabel = extractString(node \ "prefLabel")
      val note = extractString(node \ "note")
      val altLabel = extractString(node \ "altLabel")

      SkosConcept(
        concept = prefLabel,
        providedLabel = altLabel,
        note = note
      )
    })
  }

  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(metadataRoot(data ) \ "ProvidedCHO" \ "title")

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data ) \ "ProvidedCHO" \ "type")
      .flatMap(_.splitAtDelimiter(";"))

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  def agent = EdmAgent(
    name = Some("Jewish Heritage Network"),
    uri = Some(URI("http://dp.la/api/contributor/jhn"))
  )

  private def metadataRoot(data: Document[NodeSeq]): NodeSeq = data \ "metadata" \ "RDF"
}
