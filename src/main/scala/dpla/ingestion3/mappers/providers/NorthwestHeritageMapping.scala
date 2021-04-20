package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._

class NorthwestHeritageMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "nwdh"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "identifier")
      .map(_.trim)

  // SourceResource mapping
  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
  // <mods:relatedItem type=host><mods:titleInfo><mods:title>
    (data \ "relatedItem")
      .flatMap(node => getByAttribute(node, "type", "host"))
      .flatMap(collection => extractStrings(collection \ "titleInfo" \ "title"))
      .map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // when <role><roleTerm> DOES equal "contributor>
    (data \ "name")
      .filter(node => (node \ "role" \ "roleTerm").text.equalsIgnoreCase("contributor"))
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // <mods:name><mods:namePart> when <role><roleTerm> is 'creator'
    (data \ "name")
      .filter(node => (node \ "role" \ "roleTerm").text.equalsIgnoreCase("creator"))
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    // <mods:originInfo><mods:dateCreated>
    extractStrings(data \ "originInfo" \ "dateCreated")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] = {
    // <mods:note> and <mods:abstract>
    extractStrings(data \\ "mods" \ "abstract")
  }

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
  // <mods:physicalDescription><mods:extent>
    extractStrings(data \\ "mods" \ "physicalDescription" \ "extent")

  override def format(data: Document[NodeSeq]): Seq[String] =
  // <mods:genre> AND <mods:physicalDescription><mods:form>
    (extractStrings(data \\ "mods" \ "genre") ++
      extractStrings(data \\ "mods" \ "physicalDescription" \ "form"))
      .map(_.applyBlockFilter(formatBlockList))
      .filter(_.nonEmpty)

  override def identifier(data: Document[NodeSeq]): Seq[String] =
  // <mods:recordInfo> \ "recordIdentifier"
    extractStrings(data \\ "recordInfo" \ "recordIdentifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
  // <mods:language><mods:languageTerm>
    extractStrings(data \ "language" \ "languageTerm")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
  // <mods:subject><mods:geographic>
    extractStrings(data \ "subject" \ "geographic")
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
  // <mods:originInfo><mods:publisher>
    extractStrings(data \\ "mods" \ "originInfo" \ "publisher")
      .map(nameOnlyAgent)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    // <mods:subject><mods:topic>
    extractStrings(data \ "subject" \ "topic").map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): Seq[String] =
  // <mods:titleInfo><mods:title>
    extractStrings(data \ "titleInfo" \ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
  // <mods:typeOfResource>
    extractStrings(data \ "typeOfResource")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // note @type='ownershpip'
  // TODO CONFIRM MAPPING
    (data \ "note")
      .flatMap(node => getByAttribute(node, "type", "ownership"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    (data \ "accessCondition")
      .flatMap(node => getByAttribute(node, "type", "use and reproduction"))
      .flatMap(extractStrings)
      .map(URI)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <mods:location><mods:url usage="primary">
    (data \ "location" \ "url")
      .flatMap(node => getByAttribute(node, "access", "object in context"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <mods:location><mods:url access="preview">
    (data \ "location" \ "url")
      .flatMap(node => getByAttribute(node, "access", "preview"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Northwest Digital Heritage"),
    uri = Some(URI("http://dp.la/api/contributor/northwest-digital-heritage"))
  )
}
