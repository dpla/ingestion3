package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, ExtentIdentificationList}
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml.{Elem, Node, NodeSeq, Text}

class HarvardMapping
    extends XmlMapping
    with XmlExtractor
    with IngestMessageTemplates {

  override val enforceDuplicateIds = false

  // SourceResource fields

  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    for {
      titleInfoNode <- data \ "metadata" \ "mods" \ "titleInfo"
      if titleInfoNode \@ "type" == "alternative"
      titleText <- processTitleInfo(titleInfoNode)
    } yield titleText

  override def collection(
      data: Document[NodeSeq]
  ): ZeroToMany[DcmiTypeCollection] =
    extractStrings(
      data \ "metadata" \ "mods" \ "extension" \ "sets" \ "set" \ "setName"
    )
      .map(nameOnlyCollection)

  // name type="corporate"
  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    processNames(data).contributors

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    processNames(data).creators

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = {
    // date issued

    val dateIssued = (data \ "metadata" \ "mods" \ "originInfo" \ "dateIssued")
      .flatMap(extractStrings(_))
      .map(stringOnlyTimeSpan)

    // Get primary display date
    val keyDates =
      ((data \ "metadata" \ "mods" \ "originInfo" \ "dateCreated") ++
        (data \ "metadata" \ "mods" \ "originInfo" \ "dateOther"))
        .flatMap(node => getByAttribute(node, "keyDate", "yes"))
        .filterNot(node => filterAttribute(node, "encoding", "marc"))
        .flatMap(extractStrings(_))
        .map(stringOnlyTimeSpan)

    // approximate dates
    val approxDates =
      ((data \ "metadata" \ "mods" \ "originInfo" \ "dateCreated") ++
        (data \ "metadata" \ "mods" \ "originInfo" \ "dateIssued"))
        .flatMap(node => getByAttribute(node, "qualifier", "questionable"))
        .filterNot(node => filterAttribute(node, "encoding", "marc"))
        .flatMap(extractStrings(_))
        .map(str =>
          if (str.startsWith("ca. ")) {
            str
          } else
            s"ca. $str"
        )
        .map(stringOnlyTimeSpan)

    // Constructed date range
    val beginDate =
      ((data \ "metadata" \ "mods" \ "originInfo" \ "dateCreated") ++
        (data \ "metadata" \ "mods" \ "originInfo" \ "dateIssued"))
        .flatMap(node => getByAttribute(node, "point", "start"))
        .flatMap(extractStrings(_))

    val endDate =
      ((data \ "metadata" \ "mods" \ "originInfo" \ "dateCreated") ++
        (data \ "metadata" \ "mods" \ "originInfo" \ "dateIssued"))
        .flatMap(node => getByAttribute(node, "point", "end"))
        .flatMap(extractStrings(_))

    val constructedDates =
      if (beginDate.length == endDate.length)
        beginDate.zip(endDate).map { case (begin: String, end: String) =>
          EdmTimeSpan(
            originalSourceDate = Some(""), // blank original source date
            begin = Some(begin),
            end = Some(end)
          )
        }
      else Seq()

    dateIssued ++ keyDates ++ approxDates ++ constructedDates
  }

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "mods" \ "abstract") ++
      (data \ "metadata" \ "mods" \ "note")
        .filterNot(node => filterAttribute(node, "type", "funding"))
        .filterNot(node => filterAttribute(node, "type", "organization"))
        .filterNot(node => filterAttribute(node, "type", "reproduction"))
        .filterNot(node => filterAttribute(node, "type", "system details"))
        .filterNot(node =>
          filterAttribute(node, "type", "statement of responsibility")
        )
        .filterNot(node => filterAttribute(node, "type", "venue"))
        .flatMap(extractStrings)

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(
      data \ "metadata" \ "mods" \ "physicalDescription" \ "extent"
    )

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    (extractStrings(data \ "metadata" \ "mods" \ "genre") ++
      extractStrings(data \ "metadata" \ "mods" \\ "termMaterialsTech"))
      .map(
        _.applyBlockFilter(
          DigitalSurrogateBlockList.termList ++
            ExtentIdentificationList.termList
        )
      )
      .flatMap(_.splitAtDelimiter(";"))
      .filter(_.nonEmpty)

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(
      data \ "metadata" \ "mods" \ "recordInfo" \ "recordIdentifier"
    ) ++
      extractStrings(data \ "metadata" \ "mods" \ "identifier")

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    for {
      language <- data \ "metadata" \ "mods" \ "language"
      terms = language \ "languageTerm"
      data = terms.map(term => term \@ "type" -> term.text).toMap
    } yield SkosConcept(providedLabel =
      (data.get("text"), data.get("code")) match {
        case (Some(text), _)    => Some(text)
        case (None, Some(code)) => Some(code)
        case (_, _)             => None
      }
    )

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] = (
    extractStrings(data \ "metadata" \ "mods" \ "subject" \ "geographic")
      ++ extractStrings(
        data \ "metadata" \ "mods" \ "subject" \ "hierarchicalGeographic"
      )
  ).map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractStrings(data \ "metadata" \ "mods" \ "originInfo" \ "publisher")
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    for {
      relatedItem <- data \ "metadata" \ "mods" \ "relatedItem"
      if relatedItem \@ "type" == "series"
      relation <- relatedItem \ "titleInfo"
      title <- processTitleInfo(relation)
    } yield LiteralOrUri(title, isUri = false)

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    Seq("Held in the collections of Harvard University.")

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    val topicSubjects = for {
      subjectNode <- data \ "metadata" \ "mods" \ "subject"
      subject <- subjectNode \ "topic"
      subjectText = subject.text.trim
      if subjectText.nonEmpty
    } yield nameOnlyConcept(subjectText)

    val nameSubjects = for {
      subjectNode <- data \ "metadata" \ "mods" \ "subject" \ "name"
      subjectText = processNameParts(subjectNode)
    } yield nameOnlyConcept(subjectText)

    val titleSubjects = for {
      subjectNode <- data \ "metadata" \ "mods" \ "subject" \ "titleInfo"
      subjectText <- processTitleInfo(subjectNode)
    } yield nameOnlyConcept(subjectText)

    topicSubjects ++ nameSubjects ++ titleSubjects
  }

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \ "metadata" \ "mods" \ "subject" \ "temporal")
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): AtLeastOne[String] = {
    val title = for {
      titleInfoNode <- data \ "metadata" \ "mods" \ "titleInfo"
      if titleInfoNode \@ "type" != "alternative"
      titleText <- processTitleInfo(titleInfoNode)
    } yield titleText

    val collectionTitle = (data \ "metadata" \\ "relatedItem")
      .filter(node => filterAttribute(node, "displayLabel", "collection"))
      .flatMap(node => extractStrings(node \ "titleInfo" \ "title"))

    title ++ collectionTitle
  }

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "metadata" \ "mods" \ "typeOfResource") ++
      extractStrings(
        data \ "metadata" \ "mods" \ "extension" \ "librarycloud" \\ "digitalFormat"
      )

  // OreAggregation fields

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    val lookup = Map(
      "crimes" -> "Harvard Law School Library, Harvard University",
      "eda" -> "Emily Dickinson Archive",
      "lap" -> "Widener Library, Harvard University",
      "maps" -> "Harvard Map Collection, Harvard University",
      "medmss" -> "Houghton Library, Harvard University",
      "rubbings" -> "Fine Arts Library, Special Collections, Harvard University",
      "scarlet" -> "Harvard Law School Library, Harvard University",
      "scores" -> "Eda Kuhn Loeb Music Library, Harvard University",
      "ward" -> "General Artemas Ward Museum, Harvard University"
    )

    val setSpec = (for {
      setSpec <-
        data \ "metadata" \ "mods" \ "extension" \ "sets" \ "set" \ "setSpec"
    } yield setSpec.text.trim).headOption

    val setSpecAgent = lookup.get(setSpec.getOrElse("")).map(nameOnlyAgent)

    // <mods:location><physicalLocation displayLabel="Harvard repository">

    val physicalLocationAgent = (for {
      node <- data \ "metadata" \ "mods" \ "location" \ "physicalLocation"
      if node \@ "displayLabel" == "Harvard repository"
    } yield nameOnlyAgent(node.text.trim)).headOption

    // <mods:relatedItem displayLabel="collection"><location><physicalLocation displayLabel="Harvard repository">
    val hostPhysicalLocationAgent = (for {
      relatedItem <- data \ "metadata" \ "mods" \\ "relatedItem"
      if (relatedItem \@ "displayLabel") == "collection"
      node <- relatedItem \ "location" \ "physicalLocation"
      if node \@ "displayLabel" == "Harvard repository"
    } yield nameOnlyAgent(node.text.trim)).headOption

    setSpecAgent
      .orElse(physicalLocationAgent)
      .orElse(hostPhysicalLocationAgent)
      .orElse(Some(nameOnlyAgent("Harvard Library, Harvard University")))
      .toSeq
  }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def isShownAt(
      data: Document[NodeSeq]
  ): ZeroToMany[EdmWebResource] = {

    // If there is a “Harvard Art Museums” display label, always use that one
    // If there is no Harvard Art Museums, but there is more than one set name for an OAI set,
    // match the OAI setName for the collection you are harvesting with the name of the
    // collection in the display label and use that one.

    val objectInContext = "object in context"
    val access = "access"
    val displayLabel = "displayLabel"

    val artMuseumLinks = for {
      node <- data \ "metadata" \ "mods" \ "location" \ "url"
      if node \@ displayLabel == "Harvard Art Museums"
      if node \@ access == objectInContext
    } yield uriOnlyWebResource(URI(node.text.trim))

    if (artMuseumLinks.nonEmpty) return artMuseumLinks.headOption.toSeq

    val setSpec = data.get \ "about" \ "request" \@ "set"

    val setName = (for {
      set <- data.get \ "metadata" \ "mods" \ "extension" \ "sets" \ "set"
      if (set \ "setSpec").text == setSpec
    } yield (set \ "setName").text).headOption.orNull

    for {
      location <- data.get \ "metadata" \ "mods" \ "location"
      url <- location \ "url"
      if url \@ "access" == "object in context"
      if url \@ "displayLabel" == setName
    } yield uriOnlyWebResource(URI(url.text.trim))
  }

  // <mods:location><mods:url access="preview">
  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = {
    val mainUrls = for {
      node <- data \ "metadata" \ "mods" \ "location" \ "url"
      if node \@ "access" == "preview"
    } yield uriOnlyWebResource(URI(node.text.trim))

    val constituentUrls = for {
      node <- data \ "metadata" \ "mods" \ "relatedItem"
      if node \@ "type" == "constituent"
      url <- node \ "location" \ "url"
      if url \@ "access" == "preview"
    } yield uriOnlyWebResource(URI(url.text.trim))

    if (mainUrls.nonEmpty) mainUrls else constituentUrls
  }

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] =
    EdmAgent(
      name = Some("Harvard Library"),
      uri = Some(URI("http://dp.la/api/contributor/harvard"))
    )

  // utility

  override def useProviderName: Boolean = false

  override def getProviderName: Option[String] = Some("harvard")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \\ "header" \ "identifier").map(_.trim)

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  private def processTitleInfo(titleInfo: Node): ZeroToOne[String] =
    titleInfo match {
      case elem: Elem =>
        val candidates = for {
          child <- elem.child
          if child.isInstanceOf[Elem]
        } yield child.text.trim

        if (candidates.isEmpty) None else Some(candidates.mkString(" ").trim)

      case _ => None
    }

  // Helper method to get a list of creators and contributors
  private val processNames: Document[NodeSeq] => Names =
    (data: Document[NodeSeq]) => {

      val names = (for (name <- data \ "metadata" \ "mods" \ "name") yield {
        val nameString: String = processNameParts(name)
        val roleTerms =
          (name \ "role" \ "roleTerm").map(_.text.toLowerCase).distinct
        Name(nameString, roleTerms)
      }).toSet

      val creatorTypes = names.filter(name => name.roleTerm.contains("creator"))

      val categories: (Seq[Name], Seq[Name]) =
        if (names.isEmpty)
          // if names is empty, there are no creators or contributors
          (Seq(), Seq())
        else if (creatorTypes.nonEmpty)
          // if some of the names have the "creator" roleType, those are the creators
          // and the rest are the contributors
          (creatorTypes.toSeq, (names -- creatorTypes).toSeq)
        else if (
          names.head.roleTerm.isEmpty ||
          names.head.roleTerm
            .intersect(Seq("sitter", "subject", "donor", "owner"))
            .isEmpty
        )
          // otherwise, if the first name isn't a contributor role type, it's a creator,
          // and the rest are contributors
          (Seq(names.head), names.tail.toSeq)
        else
          (Seq(), names.toSeq)

      val creators = categories._1.map(x => nameOnlyAgent(x.name))
      val contributors = categories._2.map(x => nameOnlyAgent(x.name))
      Names(creators, contributors)
    }

  private def processNameParts(name: Node): String = {
    val nameParts = for {
      namePart <- name \ "namePart"
      typeAttr = (namePart \ "@type").map(_.text).headOption.getOrElse("")
      part = namePart.text
    } yield NamePart(part, typeAttr)

    if (nameParts.isEmpty) return ""

    val nameString = nameParts.tail.foldLeft(nameParts.head.part)((a, b) => {
      a + (if (b.`type` == "date") ", " else " ") + b.part
    })
    nameString
  }

  case class Name(name: String, roleTerm: Seq[String])

  private case class NamePart(part: String, `type`: String)

  private case class Names(creators: Seq[EdmAgent], contributors: Seq[EdmAgent])
}
