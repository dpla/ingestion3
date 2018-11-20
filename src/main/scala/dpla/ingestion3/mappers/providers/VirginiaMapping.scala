package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, ExtentIdentificationList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, IdMinter, Mapping, XmlExtractor}
import dpla.ingestion3.messages.{IngestMessage, IngestMessageTemplates, MessageCollector}

import dpla.ingestion3.model.DplaMapData.{ExactlyOne, LiteralOrUri, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model.{uriOnlyWebResource, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.util.{Failure, Success}
import scala.xml._


class VirginiaMapping extends Mapping[NodeSeq] with XmlExtractor with IdMinter[NodeSeq]
  with IngestMessageTemplates {

  // ID minting functions
  override def useProviderName(): Boolean = false

  // TODO: Provider name ok?
  override def getProviderName(): String = "virginia"

  // TODO: What is persistent ID?
  override def getProviderId(implicit data: Document[NodeSeq]): String =
    extractString(data \ "identifier")
      .getOrElse(throw new RuntimeException(s"No ID for record $data"))

  // Only use the first isPartOf instance
  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    extractStrings(data \ "isPartOf")
      .map(nameOnlyCollection)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "creator")
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    extractStrings(data \ "created")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "description")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "extent")

  // TODO: apply block filters a la Ohio?
  override def format(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "medium")

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "language")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(data \ "spatial")
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(data \ "publisher")
      .map(nameOnlyAgent)

  // TODO: Is this the rights statement field?
  override def rights(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "rights")

  // TODO: capitalize first char a la Ohio?
  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "subject")
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \ "temporal")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \ "type")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): URI = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "provenance")
      .map(nameOnlyAgent)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "isShownAt")
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(data \ "preview")
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  // TODO: correct URI?
  def agent = EdmAgent(
    name = Some("Digital Virginias"),
    uri = Some(URI("http://dp.la/api/contributor/virginia"))
  )

  // TODO: extentFromFormat a la Ohio?
  // TODO: subType?
}
