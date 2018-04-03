package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.enrichments.TypeMapper
import dpla.ingestion3.mappers.rdf.DCMIType
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.eclipse.rdf4j.model.IRI
import org.json4s.JsonAST
import org.json4s.JsonDSL._

import scala.xml.{Node, NodeSeq}


class NaraMapping extends Mapping[NodeSeq] with XmlExtractor with IdMinter[NodeSeq] {

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "nara"

  // itemUri will throw an exception if an ID is missing
  override def getProviderId(implicit data: Document[NodeSeq]): String =
    extractString("naId")(data).getOrElse(throw MappingException("Can't find naId"))

  def itemUri(implicit data: Document[NodeSeq]): URI =
    extractString("naId")(data).map(naId => new URI("http://catalog.archives.gov/id/" + naId))
      .getOrElse(throw MappingException("Couldn't load item url."))

  // OreAggregation
  override def dataProvider(data: Document[NodeSeq]): EdmAgent = {
    val referenceUnit = (for {
      physicalOccurrenceArray <- data \ "physicalOccurrenceArray"
      copyStatus = (physicalOccurrenceArray \\ "copyStatus" \ "termName").text
      //todo Preservation-Reproduction-Reference
      if copyStatus.contains("Reproduction-Reference") || copyStatus.contains("Preservation")
      referenceUnit = (physicalOccurrenceArray \\ "referenceUnit" \ "termName").map(_.text).headOption
    } yield referenceUnit).head

    nameOnlyAgent(referenceUnit.getOrElse("National Records and Archives Administration"))
  }

  override def dplaUri(data: Document[NodeSeq]): URI =
    mintDplaItemUri(data)

  override def isShownAt(data: Document[NodeSeq]): EdmWebResource =
    uriOnlyWebResource(itemUri(data))

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] =
    agent

  override def preview(data: Document[NodeSeq]): ZeroToOne[EdmWebResource] =
    extractPreview(data).headOption

  override def sidecar(data: Document[NodeSeq]): JsonAST.JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  // SourceResource
  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    extractCollection(data).map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractContributor(data).map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractCreator(data).map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    extractDate(data)

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings("scopeAndContentNote")(data)

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "physicalOccurrenceArray" \\ "extent")

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    extractFormat(data)

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings("naId")(data)

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \\ "languageArray" \ "language" \ "termName").map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    extractStrings(data \\ "geographicReferenceArray" \ "geographicPlaceName" \ "termName").map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractPublisher(data).map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    extractRelation(data).map(Left(_))

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    extractRights(data)

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \\ "topicalSubjectArray" \ "topicalSubject" \ "termName").map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings("title")(data)

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractTypes(data)



  // Helper methods
  private def agent = EdmAgent(
    name = Some("National Archives and Records Administration"),
    uri = Some(new URI("http://dp.la/api/contributor/nara"))
  )

  private def extractCollection(data: NodeSeq): Seq[String] = {
    val parentRecordGroupIds = for {
      prg <- data \\ "parentRecordGroup" \\ "title"
    } yield prg.text

    val parentCollectionIds = for {
      pc <- data \\ "parentCollection" \ "title"
    } yield pc.text

    if (parentRecordGroupIds.nonEmpty) parentRecordGroupIds else parentCollectionIds
  }

  private def extractContributor(data: NodeSeq): Seq[String] = {

    //TODO: not handling multiple display values. haven't found example yet.

    val organizationalContributors = for {
      org <- data \\ "organizationalContributorArray" \ "organizationalContributor"
      name = (org \ "contributor" \ "termName").text
      _type = (org \ "contributorType" \ "termName").text
      if !_type.contains("Publisher")
    } yield name

    val personalContributors = for {
      person <- data \\ "personalContributorArray" \ "personalContributor"
      name = (person \ "contributor" \ "termName").text
      //_type = (person \ "contributorType" \ "TermName").text
    } yield name

    organizationalContributors ++ personalContributors
  }

  private def extractCreator(data: NodeSeq): Seq[String] = {
    //TODO: not handling multiple display values. haven't found example yet.
    val organizationalCreators =
      for {
        creatingOrganization <- data \\ "creatingOrganizationArray" \ "creatingOrganization"
        creator <- (creatingOrganization \ "creator" \ "termName").headOption.map(_.text)
        creatorType = (creatingOrganization \ "creatorType" \ "termName").headOption.map(_.text)
        if creatorType.getOrElse("").contains("Most Recent")
      } yield creator

    val individualCreators = for {
      creator <- data \\ "creatingIndividualArray" \ "creatingIndividual" \ "creator" \ "termName"
    } yield creator.text

    if (organizationalCreators.nonEmpty)
      organizationalCreators
    else
      individualCreators
  }

  private def extractDate(data: NodeSeq): Seq[EdmTimeSpan] = {

    val coverageDates = for {
      coverageDate <- data \\ "coverageDates"
      coverageStartDate = (coverageDate \ "coverageStartDate" \ "logicalDate").headOption
      coverageEndDate = (coverageDate \ "coverageEndDate" \ "logicalDate").headOption
      //dateQualifier = coverageDate \ "dateQualifier" //todo not sure what to do with qualifier in EDTF
      if coverageStartDate.nonEmpty || coverageEndDate.nonEmpty
    } yield {
      val displayDate = getDisplayDate(coverageStartDate, coverageEndDate)
      EdmTimeSpan(
        originalSourceDate = displayDate, //this is cheating?
        begin = nodeToDateString(coverageStartDate),
        end = nodeToDateString(coverageEndDate),
        prefLabel = displayDate
      )
    }

    val copyrightDates = simpleDate(data \\ "copyrightDateArray" \ "proposableQualifiableDate")
    val productionDates = simpleDate(data \\ "productionDateArray" \ "proposableQualifiableDate")

    //todo not sure what to do with logicalDate
    val broadcastDates = simpleDate(data \\ "broadcastDateArray" \ "proposableQualifiableDate")
    val releaseDates = simpleDate(data \\ "releaseDateArray" \ "proposableQualifiableDate")

    val lastResort = for {
      inclusiveDate <- data \ "parentFileUnit" \ "parentSeries" \ "inclusiveDates"
      inclusiveStartDate = removeTime((inclusiveDate \ "inclusiveStartDate" \ "logicalDate").text)
      inclusiveEndDate = removeTime((inclusiveDate \ "inclusiveEndDate" \ "logicalDate").text)
    } yield {
      val startOption = Option(inclusiveStartDate)
      val endOption = Option(inclusiveEndDate)
      val displayDate = Option(startOption.getOrElse("unknown") + "/" + endOption.getOrElse("unknown"))
      EdmTimeSpan(originalSourceDate = displayDate, begin = startOption, end = endOption)
    }


    Seq(
      coverageDates,
      copyrightDates,
      productionDates,
      broadcastDates,
      releaseDates,
      lastResort
      //get the first non-empty one, or an empty one if they're all empty
    ).find(_.nonEmpty).getOrElse(Seq())
  }


  private def extractFormat(data: NodeSeq): Seq[String] =
    (extractStrings(data \\ "specificRecordsTypeArray" \\ "specificRecordsType" \ "termName") ++
      extractStrings(data \\ "mediaOccurrenceArray" \\ "specificMediaType" \ "termName") ++
      extractStrings(data \\ "mediaOccurrenceArray" \\ "color" \ "termName") ++
      extractStrings(data \\ "mediaOccurrenceArray" \\ "dimensions" \ "termName") ++
      extractStrings(data \\ "mediaOccurrenceArray" \\ "generalMediaType" \ "termName")).distinct

  /**
    * removes the time portion of an ISO-8601 datetime
    * @param string
    * @return string without the time, if there was one
    */
  def removeTime(string: String): String = {
    if (string.contains("T")) string.substring(0, string.indexOf('T'))
    else string
  }

  def getDisplayDate(start: Option[Node], end: Option[Node]): Option[String] = {
    if (start.isEmpty && end.isEmpty) {
      None
    } else {
      val startString = start.map(x => removeTime(x.text)).getOrElse("unknown")
      val endString = end.map(x => removeTime(x.text)).getOrElse("unknown")
      Some(s"$startString/$endString")
    }
  }

  def simpleDate(nodeSeq: NodeSeq): Seq[EdmTimeSpan] =
    nodeSeq.map(node => EdmTimeSpan(originalSourceDate = nodeToDateString(Some(node))))

  def lpad(string: String, digits: Int): String = {
    val trimString = string.trim
    if (trimString.isEmpty) string
    else if (!trimString.forall(Character.isDigit)) trimString
    else ("%0" + digits + "d").format(trimString.toInt)
  }

  def nodeToDateString(nodeOption: Option[Node]): Option[String] = nodeOption match {
    case None => None
    case Some(node) =>
      val year = lpad((node \ "year").text, 4)
      val month = lpad((node \ "month").text, 2)
      val day = lpad((node \ "day").text, 2)

      (year, month, day) match {
        case (y, m, d) if y.isEmpty => None
        case (y, m, d) if m.isEmpty => Some(year)
        case (y, m, d) if d.isEmpty => Some(s"$year-$month")
        case (y, m, d) => Some(s"$year-$month-$day")
      }
  }

  private def extractPreview(data: Document[NodeSeq]) = for {
    digitalObject <- data \ "digitalObjectArray" \ "digitalObject"
    accessFileName = (digitalObject \ "accessFilename").text
    termName = (digitalObject \ "objectType" \ "termName").text
    if termName.contains("Image") && termName.contains("JPG")
  } yield uriOnlyWebResource(new URI(accessFileName.trim))

  private def extractPublisher(data: NodeSeq): Seq[String] = {

    val orgs = for {
      org <- data \\ "organizationalContributorArray" \ "organizationalContributor"
      if (org \ "contributorType" \ "termName").text == "Publisher"
      name <- org \ "termName"
    } yield name.text

    val persons = for {
      person <- data \\ "personalContributorArray" \ "personalContributor"
      if (person \ "contributorType" \ "termName").text == "Publisher"
      name <- person \ "termName"
    } yield name.text

    orgs ++ persons
  }

  private def extractRelation(data: NodeSeq): Seq[String] = {

    val parentFileUnitRelation = for {
      parentFileUnit <- data \\ "parentFileUnit"
      value1 = (parentFileUnit \ "title").text
      value2 = (parentFileUnit \ "parentSeries" \ "title").text
      value3a = (parentFileUnit \ "parentRecordGroup" \ "title").text
      value3b = (parentFileUnit \ "parentCollection" \ "title").text
      value3 = if (value3a.isEmpty) value3b else value3a
    } yield Seq(value3, value2, value1).filter(_.nonEmpty).mkString(" ; ")

    val parentSeriesRelation = for {
      parentSeries <- data \\ "parentSeries"
      value2 = (parentSeries \ "title").text
      value3a = (parentSeries \ "parentRecordGroup" \ "title").text
      value3b = (parentSeries \ "parentCollection" \ "title").text
      value3 = if (value3a.isEmpty) value3b else value3a
    } yield Seq(value3, value2).filter(_.nonEmpty).mkString(" ; ")

    val mediaTypes = for (
      title <- data \ "microformPublicationArray" \ "microformPublication" \ "title"
    ) yield title.text

    val parents = if (parentFileUnitRelation.nonEmpty) parentFileUnitRelation
    else if (parentSeriesRelation.nonEmpty) parentSeriesRelation
    else Seq()

    (parents ++ mediaTypes).distinct
  }

  private def extractRights(data: NodeSeq): Seq[String] = for {
    useRestriction <- data \ "useRestriction"
    value1 = (useRestriction \ "note").text
    value2 = (useRestriction \ "specificUseRestrictionArray" \ "specificUseRestriction" \ "termName").text
    value3 = (useRestriction \ "status" \ "termName").text
  } yield Seq(value2, value1, value3).filter(_.nonEmpty).mkString(" ; ")


  private def extractTypes(data: NodeSeq): Seq[String] = for {
      stringType <- extractStrings(data \\ "generalRecordsTypeArray" \ "generalRecordsType" \ "termName")
      mappedType <- NaraTypeVocabEnforcer.mapNaraType(stringType)
    } yield {
      mappedType
    }
}

object NaraTypeVocabEnforcer {
  val dcmiTypes = DCMIType()
  val naraVocab: Map[String, IRI] = Map(
    "architectural and engineering drawings" -> dcmiTypes.Image,
    "artifacts" -> dcmiTypes.PhysicalObject,
    "data files" -> dcmiTypes.Dataset,
    "maps and charts" -> dcmiTypes.Image,
    "moving images" -> dcmiTypes.MovingImage,
    "photographs and other graphic materials" -> dcmiTypes.Image,
    "sound recordings" -> dcmiTypes.Sound,
    "textual records" -> dcmiTypes.Text,
    "web pages" -> dcmiTypes.InteractiveResource
  )

  val naraTypeMapper = new TypeMapper
  naraTypeMapper.addVocab(naraVocab)

  def mapNaraType(value: String): Option[String] = naraTypeMapper.enrich(value)
}
