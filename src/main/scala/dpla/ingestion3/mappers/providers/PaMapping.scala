package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, LiteralOrUri, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml.NodeSeq

class PaMapping extends XmlMapping with XmlExtractor
  with IngestMessageTemplates {

  // IdMinter methods
  override def useProviderName: Boolean = false

  // getProviderName is not implemented here because useProviderName is false

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier").map(_.trim)

  // SourceResource mapping
  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "alternative")

  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    extractStrings(metadataRoot(data) \ "isPartOf")
      .map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "metadata" \ "contributor")
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(metadataRoot(data) \ "creator")
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    extractStrings(metadataRoot(data) \ "date")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "description")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "extent")

  override def format(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "format")
      .filterNot(isDcmiType)

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(metadataRoot(data) \ "spatial")
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(metadataRoot(data) \ "publisher")
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): Seq[LiteralOrUri] =
    extractStrings(metadataRoot(data) \ "relation")
      .map(eitherStringOrUri)

  override def replacedBy(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "isReplacedBy")

  override def replaces(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "replaces")

  override def rightsHolder(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(metadataRoot(data) \ "rightsHolder")
      .map(nameOnlyAgent)

  // Label    DPLA Field    PA Field
  // Rights	  dc:rights     dcterms:rights
  override def rights(data: Document[NodeSeq]): Seq[String] =
    (metadataRoot(data) \ "rights").flatMap(r => {
      r.prefix match {
        case "dcterms" => Option(r.text)
        case _ => None
      }
    })

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(metadataRoot(data) \ "subject")
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(metadataRoot(data) \ "temporal")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "type")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(metadataRoot(data) \ "dataProvider")
      .map(nameOnlyAgent)

  //  Label             DPLA Field    PA Field
  //  Rights Statement	edm:rights		edm:rights
  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    (metadataRoot(data) \ "rights").flatMap(r => r.prefix match {
      case "edm" => Some(URI(r.text))
      case _ => None
    })

  override def intermediateProvider(data: Document[NodeSeq]): ZeroToOne[EdmAgent] =
    extractStrings(metadataRoot(data) \ "intermediateProvider")
      .map(nameOnlyAgent).headOption

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(metadataRoot(data) \ "isShownAt")
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(metadataRoot(data) \ "preview")
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("PA Digital"),
    uri = Some(URI("http://dp.la/api/contributor/pa"))
  )

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data) )

/**
    *  Get the last occurrence of the identifier property, there
    *  must be at least three dc:identifier properties for there
    *  to be a thumbnail
    *
    * @param data
    * @return
    */

  def thumbnail(implicit data: Document[NodeSeq]): ZeroToOne[EdmWebResource] = {
    val ids = extractStrings(metadataRoot(data) \ "identifier")
    if (ids.size > 2)
      Option(uriOnlyWebResource(URI(ids.last)))
    else
      None
  }

  def metadataRoot(data: Document[NodeSeq]): NodeSeq = data \ "metadata" \ "dc"
}
