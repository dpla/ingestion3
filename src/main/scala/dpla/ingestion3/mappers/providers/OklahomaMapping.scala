package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlMapping, XmlExtractor}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{nameOnlyAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class OklahomaMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "oklahoma"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

  // SourceResource mapping
  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
  // <mods:titleInfo type=alternative><mods:title>
    (data \\ "titleInfo")
      .map(node => getByAttribute(node.asInstanceOf[Elem], "type", "alternative"))
      .flatMap(titleInfo => extractStrings(titleInfo \ "title"))

  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
  // <mods:relatedItem type=host><mods:titleInfo><mods:title>
    (data \\ "relatedItem")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "host"))
      .flatMap(collection => extractStrings(collection \ "titleInfo" \ "title"))
      .map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // when <role><roleTerm> DOES equal "contributor>
    (data \\ "name")
      .filter(node => (node \ "role" \ "roleTerm").text.equalsIgnoreCase("contributor"))
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // <mods:name><mods:namePart> when <role><roleTerm> DOES NOT equal "contributor>
    (data \\ "name")
      .filter(node => !(node \ "role" \ "roleTerm").text.equalsIgnoreCase("contributor"))
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
  // <mods:originInfo><mods:dateCreated>
    extractStrings(data \\ "originInfo" \\ "dateCreated")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] = {
    // <mods:note type=content> OR <mods:abstract>
    val note = (data \\ "note")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "content"))
      .flatMap(node => extractStrings(node))
    val abs = extractStrings(data \\ "abstract")

    if (note.nonEmpty)
      note
    else
      abs
  }

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
  // <mods:physicalDescription><mods:extent>
    extractStrings(data \\ "physicalDescription" \ "extent")

  override def format(data: Document[NodeSeq]): Seq[String] =
  // <mods:genre> AND <mods:physicialDescription><mods:note>
    extractStrings(data \\ "genre") ++
      extractStrings(data \\ "physicalDescription" \ "note")

  override def identifier(data: Document[NodeSeq]): Seq[String] =
  // <mods:identifier>
    extractStrings(data \ "metadata" \ "mods" \ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
  // <mods:language><mods:languageTerm>
    extractStrings(data \\ "language" \\ "languageTerm")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
  // <mods:subject><mods:geographic>
    extractStrings(data \\ "subject" \\ "geographic")
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
  // <mods:originInfo><mods:publisher>
    extractStrings(data \\ "originInfo" \\ "publisher")
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
  // <mods:relatedItem><mods:titleInfo><mods:title> when @type DOES NOT equal "host"
    (data \\ "relatedItem")
      .filterNot({ n => filterAttribute(n, "type", "host") })
      .flatMap(n => extractStrings(n \\ "titleInfo" \\ "title"))
      .map(eitherStringOrUri)

  //  <accessCondition> when the @type="use and reproduction" attribute is not present
  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    (data \ "metadata" \ "mods" \ "accessCondition")
      .filterNot({ n => filterAttribute(n, "type", "use and reproduction") })
      .flatMap(extractStrings)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
  // <mods:subject><mods:topic>
    extractStrings(data \\ "subject" \ "topic")
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
  // <mods:subject><mods:temporal>
    extractStrings(data \\ "subject" \ "temporal")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): Seq[String] =
  // <mods:titleInfo><mods:title> when @type DOES NOT equal "alternative"
  // FIXME temporary kludge to publish title and altTitle to the search index. This is necessary because of a legacy bug
  // in ingestion1 that put all title values [alt and primary] into the title field. Since altTitle is not available in
  // the search index [only added in MAPv4] we need to shoehorn altTitle values into the title field here. This should be
  // undone when altTitle becomes available in the API and added to the portal record view.
    (data \\ "mods" \ "titleInfo")
      .filterNot({ n => filterAttribute(n, "type", "alternative") })
      .flatMap(titleInfo => extractStrings(titleInfo \ "title")) ++ // FIXME see above
    alternateTitle(data)

  override def `type`(data: Document[NodeSeq]): Seq[String] =
  // <mods:typeofresource>
    extractStrings(data \ "metadata" \ "mods" \ "typeOfResource")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    (data \ "metadata" \ "mods" \ "note")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "ownership"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
  // <mods:accessCondition type=use and reproduction xlinkhref=[#VALUE TO BE MAPPED HERE#]>
    (data \ "metadata" \ "mods" \ "accessCondition")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "use and reproduction"))
      .flatMap(node => node.attribute(node.getNamespace("xlink"), "href"))
      .flatMap(n => extractString(n.head))
      .map(URI)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <mods:location><mods:url usage="primary display" access="object in  context">
    (data \ "metadata" \ "mods" \ "location" \ "url")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "usage", "primary display"))
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "access", "object in context"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <mods:location><mods:url access="preview">
    (data \ "metadata" \ "mods" \ "location" \ "url")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "access", "preview"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("OKHub"),
    uri = Some(URI("http://dp.la/api/contributor/oklahoma"))
  )
}

