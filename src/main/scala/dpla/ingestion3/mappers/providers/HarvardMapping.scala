package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, ExtentIdentificationList}
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{EdmAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml.NodeSeq

class HarvardMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  override def useProviderName: Boolean = false

  override def getProviderName: String = "harvard"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \\ "header" \ "identifier").map(_.trim)

  // OreAggregation fields
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    for {
      node <- data \ "metadata" \ "mods" \ "location" \ "physicalLocation"
      if node \@ "type" == "repository"
    } yield nameOnlyAgent(node.text.trim)


  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    Seq("Held in the collections of Harvard University.")

  /*
    <mods:location>
    <mods:url access=”object in context” usage="primary display">
  */
  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    for {
      node <- data \ "metadata" \ "mods" \ "location" \ "url"
      if node \@ "access" == "object in context"
    } yield uriOnlyWebResource(URI(node.text.trim))

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  //<mods:location><mods:url access="preview">
  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    for {
      node <- data \ "metadata" \ "mods" \ "location" \ "url"
      if node \@ "access" == "preview"
    } yield uriOnlyWebResource(URI(node.text.trim))

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = EdmAgent(
    name = Some("Harvard Library"),
    uri = Some(URI("http://dp.la/api/contributor/harvard"))
  )

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    for {
      name <- data \ "metadata" \ "mods" \ "name"
      if (name \ "role" \ "roleTerm").text.trim == "contributor"
    } yield nameOnlyAgent((name \ "namePart").text.trim)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    for {
      name <- data \ "metadata" \ "mods" \ "name"
      if (name \ "role" \ "roleTerm").text.trim == "creator"
      nameText = (name \ "namePart").text.trim
      if nameText.nonEmpty
    } yield nameOnlyAgent(nameText)

  //TODO: dateIssued
  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    (
      extractStrings(data \ "metadata" \ "mods" \ "originInfo" \ "dateCreated")
      ++
        extractStrings(data \ "metadata" \ "mods" \ "originInfo" \ "dateIssued")
      ).map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "mods" \ "abstract")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "mods" \ "physicalDescription" \ "extent")

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "mods" \ "recordInfo" \ "recordIdentifier")

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \ "metadata" \ "mods" \ "language" \ "languageTerm")
      .map(nameOnlyConcept)

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    for {
      subjectNode <- data \ "metadata" \ "mods" \ "subject"
      subject <-
        subjectNode \ "topic" ++
          subjectNode \ "name" ++
          subjectNode \ "genre"
      subjectText = subject.text.trim
      if subjectText.nonEmpty
    } yield nameOnlyConcept(subjectText)

  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    for {
      titleInfoNode <- data \ "metadata" \ "mods" \ "titleInfo"
      if titleInfoNode \@ "type" != "alternative"
      titleNode <- titleInfoNode \ "title"
      titleText = titleNode.text.trim
      if titleText.nonEmpty
    } yield titleText

  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    for {
      titleInfoNode <- data \ "metadata" \ "mods" \ "titleInfo"
      if titleInfoNode \@ "type" == "alternative"
      titleNode <- titleInfoNode \ "title"
      titleText = titleNode.text.trim
      if titleText.nonEmpty
    } yield titleText

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "mods" \ "typeOfResource")

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \ "mods" \ "originInfo" \ "publisher")
      .map(nameOnlyAgent)

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "mods" \ "physicalDescription" \ "form")
      .map(
        _.applyBlockFilter(
          DigitalSurrogateBlockList.termList ++
            ExtentIdentificationList.termList
        )
      )
      .flatMap(_.splitAtDelimiter(";"))
      .filter(_.nonEmpty)

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    extractStrings(data \ "metadata" \ "mods" \ "subject" \ "geographic")
      .map(nameOnlyPlace)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    for {
      relatedItem <- data \ "metadata" \ "mods" \ "relatedItem"
      if relatedItem \@ "type" == "series"
      relation <- relatedItem \ "titleInfo" \ "title"
    } yield Left(relation.text.trim)

  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
    for {
      relatedItem <- data \ "metadata" \ "mods" \ "relatedItem"
      if relatedItem \@ "type" == "host"
      collectionTitle <- relatedItem \ "titleInfo" \ "title"
    } yield nameOnlyCollection(collectionTitle.text.trim)

}
