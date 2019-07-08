package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{nameOnlyAgent, _}
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class TnMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  // ID minting functions
  override def useProviderName(): Boolean = false

  override def getProviderName(): String = "tn"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
  // ingestion1 ID minting >> /select-id?prop=id&use_source=no
    extractString(data \ "header" \ "identifier")
      .map(_.trim)

  // SourceResource mapping
  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
  // <titleInfo><title type="alternative">
    (data \\ "mods" \\ "titleInfo")
      .flatMap(node => getByAttribute(node, "type", "alternative"))
      .flatMap(node => extractStrings(node \ "title"))

  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
  // For either <relatedItem displayLabel="Project"> OR <relatedItem displayLabel="collection">
  // <titleInfo><title> maps to collection title
  // <abstract> maps to collection description
    (data \\ "mods" \ "relatedItem")
      .filter(node => {
        val text = (node \ "@displayLabel").text
        text.equalsIgnoreCase("project") || text.equalsIgnoreCase("collection")
      })
      .flatMap(collection => {
        extractStrings(collection \ "titleInfo" \ "title").map(Option(_))
          .zipAll(extractStrings(collection \ "abstract").map(Option(_)), None, None)
      })
      .map( { case (title: Option[String], desc: Option[String]) => DcmiTypeCollection(title = title, description = desc)} )


  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // when <role><roleTerm> EQUALSs "contributor>
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
  // <originInfo><dateCreated> (use all instances)
    extractStrings(data \\ "mods" \ "originInfo" \ "dateCreated")
      .map(stringOnlyTimeSpan)

  override def description(data: Document[NodeSeq]): Seq[String] =
  // <mods:abstract>
    extractStrings(data \\ "metadata" \ "mods" \ "abstract")

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

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
  // <subject>
  //  <geographic>
  //    [text term] >> map to providedLabel
  //  <cartographics>
  //    <coordinates>
  //      [coordinates] >> map to coordinates
  (data \\ "mods" \ "subject")
    .flatMap(place => {
      extractStrings(place \ "geographic").map(Option(_))
        .zipAll(extractStrings(place \ "cartographics" \ "coordinates").map(Option(_)), None, None)
    })
    .map( { case (name: Option[String], coord: Option[String]) => DplaPlace(name = name, coordinates = coord)} )


  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
  // <mods:originInfo><mods:publisher>
    extractStrings(data \\ "mods" \ "originInfo" \ "publisher")
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] = {
    // <relatedItem>
    //  <title>
    //    <titlePart>[VALUE]
    //  <location>
    //    <url>[VALUE]
    //
    // when the type attribute DOES NOT equal "isReferencedBy" or "references" and
    //  the "displayLabel" attribute DOES NOT equal "Project"
    (data \\ "mods" \ "relatedItem")
      .filterNot(node => (node \ "@type").text.equalsIgnoreCase("isReferencedBy"))
      .filterNot(node => (node \ "@type").text.equalsIgnoreCase("references"))
      .filterNot(node => (node \ "@displayLabel").text.equalsIgnoreCase("project"))
      .flatMap(node =>
        extractStrings(node \ "title" \ "titlePart") ++
        extractStrings(node \ "location" \ "url"))
      .map(eitherStringOrUri)
  }

  override def replacedBy(data: Document[NodeSeq]): ZeroToMany[String] =
    // <relatedItem type="isReferencedBy">
    //   <title>
    //     <titlePart>[VALUE]
    //   <location>
    //     <url>[VALUE]
    (data \\ "mods" \ "relatedItem")
      .filter(node => (node \ "@type").text.equalsIgnoreCase("isReferencedBy"))
      .flatMap(node =>
        extractStrings(node \ "title" \ "titlePart") ++
          extractStrings(node \ "location" \ "url"))

  override def replaces(data: Document[NodeSeq]): ZeroToMany[String] =
    // <relatedItem type="references">
    //   <title>
    //     <titlePart>[VALUE]
    //   <location>
    //     <url>[VALUE]
    (data \\ "mods" \ "relatedItem")
      .filter(node => (node \ "@type").text.equalsIgnoreCase("references"))
      .flatMap(node =>
        extractStrings(node \ "title" \ "titlePart") ++
        extractStrings(node \ "location" \ "url"))

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    // <mods:accessCondition>
    (data \\ "mods" \ "accessCondition")
      .flatMap(node => getByAttribute(node, "type", "local rights statement")) // TODO should this filter be in place?
      .flatMap(extractStrings)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] = {
    // <subject>
    //   <topic>
    //   <name>
    //   <titleInfo>
    //     <title>
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
  // <mods:titleInfo><mods:title> where titleInfo has no attributes
    (data \ "metadata" \ "mods" \ "titleInfo")
      .filter(node => node.attributes.isEmpty)
      .flatMap(node => extractStrings(node \ "title"))

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
    (data \\ "mods" \ "recordInfo" \ "recordContentSource")
      .flatMap(extractStrings)
      .map(nameOnlyAgent)

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] =
  // <accessCondition type="use and reproduction" xlink="[value to be mapped is here]">
    (data \ "metadata" \ "mods" \ "accessCondition")
      .flatMap(node => getByAttribute(node, "type", "use and reproduction"))
      .flatMap(node => node.attribute(node.getNamespace("xlink"), "href"))
      .flatMap(extractStrings(_))
      .map(URI)

  override def intermediateProvider(data: Document[NodeSeq]): ZeroToOne[EdmAgent] =
    // <note displayLabel=“Intermediate Provider”>
    (data \\ "mods" \ "note")
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
    ((data \\ "mods" \ "location" \ "url")
      .flatMap(node => getByAttribute(node, "access", "raw object"))
      .flatMap(extractStrings) ++
    // Add IIIF manifests
    (data \\ "mods" \ "location" \ "url")
      .flatMap(node => getByAttribute(node, "note", "iiif-manifest"))
      .flatMap(extractStrings)
    ).map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <location><url access="preview">
    (data \\ "mods" \ "location" \ "url")
      .flatMap(node => getByAttribute(node, "access", "preview"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Digital Library of Tennessee"),
    uri = Some(URI("http://dp.la/api/contributor/tennessee"))
  )
}
