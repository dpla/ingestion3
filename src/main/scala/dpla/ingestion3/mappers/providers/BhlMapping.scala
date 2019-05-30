package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, XmlMapping, XmlExtractor}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._


class BhlMapping extends XmlMapping with XmlExtractor {

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: String = "bhl"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \\ "header" \ "identifier")

  // SourceResource mapping

  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
  // <mods:relatedItem @type="series"><titleInfo><title>
    (data \\ "metadata" \ "mods" \ "relatedItem")
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "type", "series"))
      .flatMap(n => extractStrings(n \ "titleInfo" \ "title"))
      .map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    // <mods:name><mods:namePart> when <mods:role><mods:roleTerm> equals contributor
    // concatenate <mods:namePart> when @type does not equal "affiliation", "displayForm", "description", or "role"

    val badTypes = List("affiliation", "displayForm", "description", "role")

    val parentNodes: NodeSeq = (data \\ "metadata" \ "mods" \ "name")
      .flatMap(node => node.filter(n => (n \\ "roleTerm").text.equalsIgnoreCase("contributor")))

    val names: Seq[String] = parentNodes.map(n =>
      (n \ "namePart")
        .filterNot(n => badTypes.contains(n \@ "type"))
        .flatMap(extractStrings)
        .map(_.cleanupEndingCommaAndSpace)
        .mkString(", ")
    )

    names.map(nameOnlyAgent)
  }

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    // <mods:name><mods:namePart> when <mods:role><mods:roleTerm> equals creator
    // concatenate <mods:namePart> when @type does not equal "affiliation", "displayForm", "description", or "role"

    val badTypes = List("affiliation", "displayForm", "description", "role")

    val parentNodes: NodeSeq = (data \\ "metadata" \ "mods" \ "name")
      .flatMap(node => node.filter(n => (n \\ "roleTerm").text.equalsIgnoreCase("creator")))

    val names: Seq[String] = parentNodes.map(n =>
      (n \ "namePart")
        .filterNot(n => badTypes.contains(n \@ "type"))
        .flatMap(extractStrings)
        .map(_.cleanupEndingCommaAndSpace)
        .mkString(", ")
    )

    names.map(nameOnlyAgent)
  }

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = {
    // <mods:originInfo><mods:dateIssued keyDate="yes"> OR
    // <mods:originInfo><mods:dateOther type="issueDate" keyDate="yes>
    // Records should have only one or the other

    val dateIssued: Seq[String] = (data \\ "metadata" \ "mods" \ "originInfo" \ "dateIssued")
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "keyDate", "yes"))
      .flatMap(extractStrings)

    val dateOther: Seq[String] = (data \\ "metadata" \ "mods" \ "originInfo" \ "dateOther")
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "keyDate", "yes"))
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "type", "issueDate"))
      .flatMap(extractStrings)

    getDateValues(dateIssued ++ dateOther)
      .map(stringOnlyTimeSpan)
  }

  def getDateValues(dates: Seq[String]): Seq[String] = {
    val sorted = dates.sorted

    sorted.reverse.headOption match { // get last date in sequence
      case Some(d) =>
        if (d.contains("/") || d.contains("-")) Seq(d) // date is already range so just use it
        else if (dates.size > 1) Seq(sorted.head + "-" + d) // create range using first and last date
        else Seq(d) // use only available date
      case None => Seq() // no date
    }
  }

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
  // <mods:note @type="content">
    (data \\ "metadata" \ "mods" \ "note")
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "type", "content"))
      .flatMap(extractStrings)
      .map(_.stripSuffix(","))

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
  // <mods:physicalDescription><mods:form @authority="marcform">
    (data \\ "metadata" \ "mods" \ "physicalDescription" \ "form")
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "authority", "marcform"))
      .flatMap(extractStrings)

  // TODO: Map this?  None in i3 reports
  override def genre(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
  // <mods:genre authority="marcgt">
    (data \\ "metadata" \ "mods" \ "genre")
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "authority", "marcgt"))
      .flatMap(extractStrings)
      .map(nameOnlyConcept)

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \\ "metadata" \ "mods" \ "identifier")

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
  // <mods:language><mods:languageTerm @type="text" @authority="iso639-2b">
    (data \\ "metadata" \ "mods" \ "language" \ "languageTerm")
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "type", "text"))
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "authority", "iso639-2b"))
      .flatMap(extractStrings)
      .map(nameOnlyConcept)

  // TODO: split at ; ? only affects a few records
  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    extractStrings(data \\ "metadata" \ "mods" \ "subject" \ "geographic")
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    // <mods:originInfo><mods:publisher>
    // If <mods:originInfo><mods:place><mods:placeName> exists, add it to beginning of publisher name string

    val names: Seq[String] = extractStrings(data \\ "metadata" \ "mods" \ "originInfo" \ "publisher")

    val places: Seq[String] = extractStrings(data \\ "metadata" \ "mods" \ "originInfo" \ "place" \ "placeTerm")

    val publishers: Seq[String] = names.zipWithIndex.map{ case(n, i) => {
      if (places.lift(i).isDefined) places(i) + " " + n else n
    }}

    publishers.map(nameOnlyAgent)
  }

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    extractStrings(data \\ "metadata" \ "mods" \ "relatedItem" \ "titleInfo" \ "title")
      .map(eitherStringOrUri)

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings(data \\ "metadata" \ "mods" \ "accessCondition")

  // TODO: split at ; ?
  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    // <mods:subject><mods:topic> AND <mods:subject><mods:genre>

    val topic = extractStrings(data \\ "metadata" \ "mods" \ "subject" \ "topic")
    val genre = extractStrings(data \\ "metadata" \ "mods" \ "subject" \ "genre")

    (topic ++ genre).map(nameOnlyConcept)
  }

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \\ "metadata" \ "mods" \ "subject" \ "temporal")
      .map(stringOnlyTimeSpan)

  // TODO: split at ; ?
  override def title(data: Document[NodeSeq]): ZeroToMany[String] = {
    // <mods:titleInfo> <mods:title>
    // when <mods:titleInfo> does not have @type

    val parentNode = (data \\ "metadata" \ "mods" \ "titleInfo")
      .filter(n => (n \@ "type").isEmpty)

    (parentNode \ "title")
      .flatMap(extractStrings)
      .map(_.cleanupEndingPunctuation)
  }

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \\ "metadata" \ "mods" \ "typeOfResource")

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
  // <mods:note @type="ownership">
    (data \\ "metadata" \ "mods" \ "note")
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "type", "ownership"))
      .flatMap(extractStrings)
      .map(nameOnlyAgent)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <mods:location><mods:url @access="raw object" @usage="primary">
    (data \\ "metadata" \ "mods" \ "location" \ "url")
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "access", "raw object"))
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "usage", "primary"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // <mods:location><mods:url @access="object in context" @usage="primary display">
    (data \\ "metadata" \ "mods" \ "location" \ "url")
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "access", "object in context"))
      .flatMap(n => getByAttribute(n.asInstanceOf[Elem], "usage", "primary display"))
      .flatMap(extractStrings)
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("Biodiversity Heritage Library"),
    uri = Some(URI("http://dp.la/api/contributor/bhl"))
  )
}
