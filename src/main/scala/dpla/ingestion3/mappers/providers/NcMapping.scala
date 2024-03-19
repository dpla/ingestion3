package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.enrichments.TaggingUtils._
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{nameOnlyAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class NcMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("digitalnc")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")
      .map(_.trim)

  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    (data \\ "relatedItem")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "displayLabel", "collection"))
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "host"))
      .flatMap(collection => extractStrings(collection \ "titleInfo" \ "title"))
      .map(nameOnlyCollection)

  // SourceResource mapping
  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // when <role><roleTerm> DOES equal "contributor>
    (data \\ "mods" \ "name")
      .filter(node => (node \ "role" \ "roleTerm").text.equalsIgnoreCase("contributor"))
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // <mods:name><mods:namePart> when <role><roleTerm> is 'creator'
    (data \\ "mods" \ "name")
      .filter(node => (node \ "role" \ "roleTerm").text.equalsIgnoreCase("creator"))
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = {
    (data \\ "mods" \ "originInfo" \ "dateCreated")
      .filter(node => filterAttribute(node, "keyDate", "yes"))
      .flatMap(extractStrings)
      .map(stringOnlyTimeSpan)
  }

  override def description(data: Document[NodeSeq]): Seq[String] =
  // <mods:note type='content'>
    (data \\ "mods" \ "note")
      .filter(node => filterAttribute(node, "type", "content"))
      .flatMap(extractStrings)

  override def format(data: Document[NodeSeq]): Seq[String] =
  // <physicalDescription><form>
    extractStrings(data \\ "mods" \ "physicalDescription" \ "form")

  override def identifier(data: Document[NodeSeq]): Seq[String] =
  // <mods:identifier>
    extractStrings(data \\ "mods" \ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
  // <mods:language><mods:languageTerm>
    extractStrings(data \\ "mods" \ "language" \ "languageTerm")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
  // <mods:subject><mods:geographic>
    extractStrings(data \\ "mods" \ "subject" \ "geographic")
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
  // <mods:originInfo><mods:publisher>
    extractStrings(data \\ "mods" \ "originInfo" \ "publisher")
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
  // <relatedItem><location><url> and/or <relatedItem><titleInfo><title>
    (extractStrings(data \\ "mods" \ "relatedItem" \ "location" \ "url") ++
      extractStrings(data \\ "mods" \ "relatedItem" \ "titleInfo" \ "title"))
      .map(eitherStringOrUri)

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
  // all values except <accessCondition type="use and reproduction">
    (data \\ "mods" \ "accessCondition")
      .filterNot(node => filterAttribute(node, "type", "use and reproduction"))
      .flatMap(extractStrings)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
  // <mods:subject><mods:topic>
    extractStrings(data \\ "mods" \ "subject" \ "topic")
      .map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): Seq[String] =
  // <mods:titleInfo><mods:title>
    extractStrings(data \\ "mods" \ "titleInfo" \ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] =
  // <mods:genre>
    extractStrings(data \\ "mods" \ "genre")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // first <note type="ownership">
    (data \\ "mods" \ "note")
      .filter(node => filterAttribute(node, "type", "ownership"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)
      .take(1)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
  // <accessCondition type="use and reproduction">
    (data \\ "mods" \ "accessCondition")
      .filter(node => filterAttribute(node, "type", "use and reproduction"))
      .flatMap(extractStrings)
      .map(URI)

  override def intermediateProvider(data: Document[NodeSeq]): ZeroToOne[EdmAgent] =
  // second <note type=ownership> if it exists
    (data \\ "mods" \ "note")
      .filter(node => filterAttribute(node, "type", "ownership"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)
      .lift(1)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <location><url usage="primary display" access="object in context">
    (data \\ "mods" \ "location" \ "url")
      .filter(node => filterAttribute(node, "usage", "primary display"))
      .filter(node => filterAttribute(node, "access", "object in context"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <location><url access="preview">
    (data \\ "mods" \ "location" \ "url")
      .filter(node => filterAttribute(node, "access", "preview"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  override def tags(data: Document[NodeSeq]): ZeroToMany[URI] =
    description(data).flatMap(_.applyAviationTags)

  // Helper method
  def agent = EdmAgent(
    name = Some("North Carolina Digital Heritage Center"),
    uri = Some(URI("http://dp.la/api/contributor/digitalnc"))
  )
}
