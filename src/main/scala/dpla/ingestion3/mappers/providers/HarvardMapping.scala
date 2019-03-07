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

import scala.xml.{Elem, Node, NodeSeq}

class HarvardMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  // SourceResource fields

  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    for {
      titleInfoNode <- data \ "metadata" \ "mods" \ "titleInfo"
      if titleInfoNode \@ "type" == "alternative"
      titleText <- processTitleInfo(titleInfoNode)
    } yield titleText

  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] =
    extractStrings(data \ "metadata" \ "mods" \ "extension" \ "sets" \ "set" \ "setName")
    .map(nameOnlyCollection)

  //name type="corporate"
  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    for {
      name <- data \ "metadata" \ "mods" \ "name"
      if (name \ "role" \ "roleTerm").text.trim == "contributor"
    } yield nameOnlyAgent((name \ "namePart").text.trim)

  //todo: comma space for dates attached to names
  //todo: multiple nameparts with no type period space
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

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "mods" \ "recordInfo" \ "recordIdentifier")

  //TODO fix language (code vs. displayValue)
  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \ "metadata" \ "mods" \ "language" \ "languageTerm")
      .map(nameOnlyConcept) //todo code + text

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] = (
    extractStrings(data \ "metadata" \ "mods" \ "subject" \ "geographic")
      ++ extractStrings(data \ "metadata" \ "mods" \ "subject" \ "hierarchicalGeographic")
    ).map(nameOnlyPlace)


  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \ "mods" \ "originInfo" \ "publisher")
      .map(nameOnlyAgent)


  //TODO concatenate subparts
  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    for {
      relatedItem <- data \ "metadata" \ "mods" \ "relatedItem"
      if relatedItem \@ "type" == "series"
      relation <- relatedItem \ "titleInfo" \ "title"
    } yield Left(relation.text.trim)


  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    Seq("Held in the collections of Harvard University.")


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


  //todo
  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = super.temporal(data)

  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    for {
      titleInfoNode <- data \ "metadata" \ "mods" \ "titleInfo"
      if titleInfoNode \@ "type" != "alternative"
      titleNode <- titleInfoNode \ "title"
      titleText <- processTitleInfo(titleNode)
      if titleText.nonEmpty
    } yield titleText

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "mods" \ "typeOfResource")


  // OreAggregation fields

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    for {
      node <- data \ "metadata" \ "mods" \ "location" \ "physicalLocation"
      if node \@ "type" == "repository"
    } yield nameOnlyAgent(node.text.trim)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  /*
    <mods:location>
    <mods:url access=”object in context” usage="primary display">
  */
  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    for {
      node <- data \ "metadata" \ "mods" \ "location" \ "url"
      if node \@ "access" == "object in context"
    } yield uriOnlyWebResource(URI(node.text.trim))

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

  //utility

  override def useProviderName: Boolean = false

  override def getProviderName: String = "harvard"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \\ "header" \ "identifier").map(_.trim)

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  private def name(name: Node): ZeroToOne[String] = name match {
    case elem: Elem =>
    case _ => None
  }

  private def processTitleInfo(titleInfo: Node): ZeroToOne[String] = titleInfo match {
    case elem: Elem =>
      val candidates = for {
        elementName <- Seq("nonSort", "title", "subTitle", "partName", "partNumber")
        stringVal <- extractStrings(elementName)(elem).headOption
      } yield stringVal.trim

      if (candidates.isEmpty) None else Some(candidates.mkString(" "))

    case _ => None
  }


  private def processNames(data: Document[NodeSeq]): Names = {

    val names = extractNames(data).toSet
    val creatorTypes = names.filter(name => name.roleTerm.contains("creator"))

    val categories: Tuple2[_root_.scala.Equals, _root_.scala.collection.Iterable[HarvardMapping.this.Name] with (Function1[HarvardMapping.this.Name with Int, Any])] =
      if (names.isEmpty)
        (Seq(), Seq())
      else if (creatorTypes.nonEmpty)
        (creatorTypes, names -- creatorTypes)
      else if (names.head.roleTerm.isEmpty || names.head.roleTerm.intersect(Seq("sitter", "subject", "donor", "owner")).isEmpty)
        (names.head, names.tail)
      else
        (Seq(), names.toSeq)

    Names(categories._1.map(x => nameOnlyAgent(x.name)), categories._2.map(nameOnlyAgent(._name)))

  }

  private def extractNames(data: Document[NodeSeq]) =
    for (name <- data \ "metadata" \ "mods" \ "name") yield {

      val nameParts = for {
        namePart <- name \ "namePart"
        typeAttr = (namePart \ "@type").map(_.text).headOption.getOrElse("")
        part = namePart.text
      } yield NamePart(part, typeAttr)

      val nameString = nameParts.foldLeft("")(
        (a, b) => {a + (if (b.`type` == "date") ". " else " ") + b.part}
      )

      val roleTerms = (name \ "role" \ "roleTerm").map(_.text.toLowerCase).distinct

      Name(nameString, roleTerms)
  }

  case class Name(name: String, roleTerm: Seq[String])
  case class NamePart(part: String, `type`: String)
  case class Names(contributors: Seq[EdmAgent], creators: Seq[EdmAgent])

}
