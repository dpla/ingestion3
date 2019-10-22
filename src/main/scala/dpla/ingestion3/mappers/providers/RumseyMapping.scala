package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, LiteralOrUri, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils

import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml.NodeSeq

class RumseyMapping extends XmlMapping with XmlExtractor
  with IngestMessageTemplates {

  // IdMinter methods
  override def useProviderName: Boolean = true

  // FYI - David Rumsey IDs will/have changed in i3. Ingestion1 used first absolute URI in dc:identifier property
  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier").map(_.trim)

  // SourceResource mapping
  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "alternative")

  // done, sbw. hard coded
  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    Seq(DcmiTypeCollection(
        title = Some("David Rumsey Map Collection"),
        description = Some("The historical map collection has over 38,000 maps and images online. The collection focuses on rare 18th and 19th century North American and South American maps and other cartographic materials. Historic maps of the World, Europe, Asia, and Africa are also represented.")
    ))

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(metadataRoot(data) \ "contributor")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(metadataRoot(data) \ "creator")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    extractStrings(metadataRoot(data) \ "date")
      .flatMap(_.splitAtDelimiter(";"))
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "description")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "extent")

  override def format(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "format")
      .flatMap(_.splitAtDelimiter(";"))
      .filterNot(isDcmiType)

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(metadataRoot(data) \ "language")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(metadataRoot(data) \ "coverage")
      .flatMap(_.splitAtDelimiter("--"))
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractStrings(metadataRoot(data) \ "publisher")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): Seq[LiteralOrUri] =
    extractStrings(metadataRoot(data) \ "relation")
      .flatMap(_.splitAtDelimiter(";"))
      .map(eitherStringOrUri)

  override def replacedBy(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "isReplacedBy")

  override def replaces(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(metadataRoot(data) \ "replaces")

  override def rightsHolder(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(metadataRoot(data) \ "rightsHolder")
      .map(nameOnlyAgent)

  // done, sbw. hard-coded
  override def rights(data: Document[NodeSeq]): Seq[String] =
    Seq("Creative Commons Attribution-NonCommercial-ShareAlike 3.0 Unported")

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(metadataRoot(data) \ "subject")
      .flatMap(_.splitAtDelimiter(";"))
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(metadataRoot(data) \ "temporal")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings(metadataRoot(data) \ "title")

  //  hardcoded to `image`
  override def `type`(data: Document[NodeSeq]): Seq[String] = Seq("image")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  // done, sbw hard-coded
  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    Seq(EdmAgent(name = Some("David Rumsey")))

  // done, sbw hard-coded
  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    Seq(URI("https://creativecommons.org/licenses/by-nc-sa/3.0/"))

  // take the first URL from dc:identifier
  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(metadataRoot(data) \ "identifier")
      .filter(Utils.isUrl)
      .take(1)
      .map(_.replace("74.126.224.22", "www.davidrumsey.com")) // replace hard coded IP address in value
      .map(stringOnlyWebResource)


  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  // second occurrence of dc:identifier
  // done, sbw
  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractStrings(metadataRoot(data) \ "identifier")
      .slice(1, 2)
      .map(stringOnlyWebResource)

  // done, sbw
  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("David Rumsey"),
    uri = Some(URI("http://dp.la/api/contributor/david_rumsey"))
  )

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data) )

  // Helper methods
  def metadataRoot(data: Document[NodeSeq]): NodeSeq = data \ "metadata" \ "dc"
}
