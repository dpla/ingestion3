package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{nameOnlyAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class TnMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "tn"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")
      .map(_.trim)

  // SourceResource mapping
  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    // <titleInfo><title type="alternative">
    (data \\ "mods" \\ "titleInfo" \ "title")
      .flatMap(node => getByAttribute(node, "type", "alternative"))
      .flatMap(extractStrings)

  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
    // <relatedItem displayLabel="Project"><titleInfo><title> AND <relatedItem displayLabel="collection"><titleInfo><title>
    // TODO implement mapping of abstract to collection description
    (data \\ "mods" \ "relatedItem")
      .filter(node => {
        val text = (node \ "@displayLabel").text
        text.equalsIgnoreCase("project") || text.equalsIgnoreCase("collection")
      })
      .flatMap(collection => extractStrings(collection \ "titleInfo" \ "title"))
      .map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // when <role><roleTerm> DOES equal "contributor>
    (data \\ "mods" \ "name")
      .filter(node => (node \ "role" \ "roleTerm").text.equalsIgnoreCase("contributor"))
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // <name><namePart>[value]</namePart><role><roleTerm>Creator</roleTerm></role></name>
    (data \\ "mods" \ "name")
      .filter(node => (node \ "role" \ "roleTerm").text.equalsIgnoreCase("creator"))
      .flatMap(n => extractStrings(n \ "namePart"))
      .map(nameOnlyAgent)


  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    // <originInfo><dateCreated> (use all instances. Some have qualifier indicating EDTF format and keyDate, others are qualified as "approximate")
    extractStrings(data \\ "mods" \ "originInfo" \ "dateCreated")
      .map(stringOnlyTimeSpan)



  override def description(data: Document[NodeSeq]): Seq[String] =
    // <mods:abstract>
    extractStrings(data \\ "mods" \ "abstract")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    // <mods:physicalDescription><mods:extent>
    extractStrings(data \\ "mods" \ "physicalDescription" \ "extent")

  override def format(data: Document[NodeSeq]): Seq[String] =
    // <mods:physicalDescription><mods:form>
    extractStrings(data \\ "mods" \ "physicalDescription" \ "form")

  override def identifier(data: Document[NodeSeq]): Seq[String] =
  // <mods:identifier>
    extractStrings(data \\ "mods" \ "identifier")

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
  // <language><languageTerm type="code" authority="iso639-2b">
    (data \\ "mods" \ "language" \ "languageTerm")
      .flatMap(node => getByAttribute(node, "type", "code"))
      .flatMap(node => getByAttribute(node, "authority", "iso639-2b"))
      .flatMap(extractStrings)
      .map(nameOnlyConcept)

//  override def place(data: Document[NodeSeq]): Seq[DplaPlace] = ???
//  // "<subject><geographic authority="""" valueURI="""">[text term]</geographic>" map to providedLabel
//  // "<subject><cartographics><coordinates>[coordinates]</coordinates> map to coordinates
//  // TODO

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
  // <mods:originInfo><mods:publisher>
    extractStrings(data \\ "mods" \ "originInfo" \ "publisher")
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] = {
    // <relatedItem>
    //  <title>
    //    <titlePart>[VALUE]
    // <relatedItem>
    //  <location>
    //    <url>[VALUE]
    //
    // when the type attribute DOES NOT equal "isReferencedBy" or "references" and
    //  the "displayLabel" attribute DOES NOT equal "Project"
    (data \\ "mods" \ "relatedItem")
      .filterNot(node => {
        val text = (node \ "@type").text
        text.equalsIgnoreCase("isReferencedBy") || text.equalsIgnoreCase("references")
      })
      .filterNot(node => (node \ "@displayLabel").text.equalsIgnoreCase("project"))
      .flatMap(extractStrings)
  }

  override def replacedBy(data: Document[NodeSeq]): ZeroToMany[String] =
    // <relatedItem type="isReferencedBy"><title><titlePart>[VALUE]<relatedItem type="isReferencedBy"><location><url>[VALUE]
    (data \\ "mods" \ "relatedItem")
      .flatMap(node => getByAttribute(node, "type", "isReferencedBy"))
      .flatMap(node => extractStrings(node \ "title" \ "titlePart") ++ extractStrings(node \ "location" \ "url"))

  override def replaces(data: Document[NodeSeq]): ZeroToMany[String] =
    // <relatedItem type="references"><title><titlePart> [VALUE] <relatedItem type="references"><location><url>[VALUE]
    (data \\ "mods" \ "relatedItem")
      .flatMap(node => getByAttribute(node, "type", "references"))
      .flatMap(node => extractStrings(node \ "title" \ "titlePart") ++ extractStrings(node \ "location" \ "url"))

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    // <mods:accessCondition>
    extractStrings(data \\ "mods" \ "accessCondition")

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] = {
    // <subject>
    //   <topic>
    //   <name>
    //   <titleInfo><title>
    //   <genre>
    //   <occupation>

    (extractStrings(data \\ "mods" \ "subject" \ "topic") ++
      extractStrings(data \\ "mods" \ "subject" \ "name") ++
      extractStrings(data \\ "mods" \ "subject" \ "genre") ++
      extractStrings(data \\ "mods" \ "subject" \ "titleInfo" \ "title") ++
      extractStrings(data \\ "mods" \ "subject" \ "occupation"))
      .map(nameOnlyConcept)
  }

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
  // <mods:subject><mods:temporal>
    extractStrings(data \\ "mods" \ "subject" \ "temporal")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): Seq[String] =
  // <mods:titleInfo><mods:title>
    extractStrings(data \\ "mods" \ "titleInfo" \ "title")

  override def `type`(data: Document[NodeSeq]): Seq[String] = {
  // <typeOfResource>; if not present use <physicalDescription><form>
    val types = extractStrings(data \\ "mods" \ "typeOfResource")
    val form = extractStrings(data \\ "mods" \ "physicalDescription" \ "form")

    if (types.nonEmpty)
      types
    else
      form
  }

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // <recordInfo><recordContentSource>
    extractStrings(data \\ "mods" \ "recordInfo" \ "recordContentSource")
      .map(nameOnlyAgent)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
    // <accessCondition type="use and reproduction" xlink="[value to be mapped is here]">
    (data \ "metadata" \ "mods" \ "accessCondition")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "type", "use and reproduction"))
      .flatMap(node => node.attribute(node.getNamespace("xlink"), "href"))
      .flatMap(extractStrings(_))
      .map(URI)

  override def intermediateProvider(data: Document[NodeSeq]): ZeroToOne[EdmAgent] =
    // <note displayLabel=“Intermediate Provider”>
    (data \\ "mods" \ "node" \ "recordContentSource")
      .flatMap(node => getByAttribute(node, "displayLabel", "Intermediate Provider"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)
      .headOption

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <location><url usage = starts with "primary" access="object in context">
    (data \\ "mods" \ "location" \ "url")
      .filter(node => (node \ "@usage").text.startsWith("primary"))
      .flatMap(node => getByAttribute(node, "access", "object in context"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def `object`(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    // <location><url access="raw object">
    (data \\ "mods" \ "location" \ "url")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "access", "raw object"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <location><url access="preview">
    (data \\ "mods" \ "location" \ "url")
      .flatMap(node => getByAttribute(node.asInstanceOf[Elem], "access", "preview"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Tennessee Digital Library"),
    uri = Some(URI("http://dp.la/api/contributor/tn"))
  )
}
