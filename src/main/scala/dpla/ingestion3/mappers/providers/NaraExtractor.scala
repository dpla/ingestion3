package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.enrichments.{DcmiTypeMapper, VocabEnforcer}
import dpla.ingestion3.mappers.rdf.DCMIType
import dpla.ingestion3.mappers.utils.{Document, IdMinter, Mapping, XmlExtractor}
import dpla.ingestion3.model.DplaMapData.{ExactlyOne, LiteralOrUri, ZeroToMany, ZeroToOne}
import dpla.ingestion3.model.{EdmAgent, EdmTimeSpan}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.eclipse.rdf4j.model.IRI
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml.{Node, NodeSeq}

class NaraExtractor extends Mapping[NodeSeq] with XmlExtractor with IdMinter[NodeSeq] {

  // implicit val xml: NodeSeq = XML.loadString(rawData)

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "nara"

  // itemUri will throw an exception if an ID is missing
  override def getProviderId(implicit data: Document[NodeSeq]): String = itemUri.toString

  def itemUri(implicit data: Document[NodeSeq]): URI =
    extractString("naId")(data).map(naId => new URI("http://catalog.archives.gov/id/" + naId))
      .getOrElse(throw new RuntimeException("Couldn't load item url."))

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): URI = mintDplaItemUri(data)

  override def dataProvider(xml: Document[NodeSeq]): EdmAgent = {
    val referenceUnit = (for {
      itemPhysicalOccurrence <- xml \ "physicalOccurrenceArray" \ "itemPhysicalOccurrence"
      copyStatus = (itemPhysicalOccurrence \ "copyStatus" \ "termName").text
      if copyStatus == "Reproduction-Reference" || copyStatus == "Preservation"
      referenceUnit = (itemPhysicalOccurrence \ "referenceUnit" \ "termName").text
    } yield referenceUnit).headOption

    nameOnlyAgent(referenceUnit.getOrElse("National Records and Archives Administration"))
  }

  override def isShownAt(data: Document[NodeSeq]) = uriOnlyWebResource(itemUri(data))

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def preview(data: Document[NodeSeq]): ZeroToOne[EdmWebResource] =
    extractString(data \ "digitalObjectArray" \ "digitalObject" \ "thumbnailFilename")
      .map(new URI(_))
      .map(uriOnlyWebResource)

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data) )


  // SourceResource
  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    extractCollection(data).map(nameOnlyCollection)

  override def contributor(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractContributor(data).map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractCreator(data).map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): Seq[EdmTimeSpan] =
    extractDate(data)

  override def description(data: Document[NodeSeq]): Seq[String] =
    extractStrings("scopeAndContentNote")(data)

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings("extent")(data)

  override def format(data: Document[NodeSeq]): Seq[String] =
    extractStrings(data \\ "specificRecordsTypeArray" \ "specificRecordsType" \ "termName")

  override def genre(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \ "metadata" \\ "type").distinct.map(nameOnlyConcept)

  override def identifier(data: Document[NodeSeq]): Seq[String] =
    extractStrings("naId")(data)

  override def language(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data \\ "languageArray" \ "language" \ "termName").map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): Seq[DplaPlace] =
    extractStrings(data \\ "geographicReferenceArray" \ "geographicPlaceName" \ "termName").map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): Seq[EdmAgent] =
    extractPublisher(data).map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): Seq[LiteralOrUri] =
    extractRelation(data).map(Left(_))

  override def rights(data: Document[NodeSeq]): Seq[String] =
    extractRights(data)

  override def subject(data: Document[NodeSeq]): Seq[SkosConcept] =
    extractStrings(data  \\ "topicalSubjectArray" \ "topicalSubject" \ "termName").map(nameOnlyConcept)

  override def title(data: Document[NodeSeq]): Seq[String] =
    extractStrings("title")(data)

  override def `type`(data: Document[NodeSeq]): Seq[String] =
    extractTypes(data)

  // Helper methods

  def agent = EdmAgent(
    name = Some("National Archives and Records Administration"),
    uri = Some(new URI("http://dp.la/api/contributor/nara"))
  )

  def extractCollection(xml: NodeSeq): Seq[String] = {
    val parentRecordGroupIds = for {
      prg <- xml \ "parentRecordGroup"
      prgId <- prg \ "naId" :: prg \ "title" :: prg \ "recordGroupNumber" :: Nil
    } yield prgId.text

    val parentCollectionIds = for {
      pc <- xml \ "parentCollection"
      pcId <- pc \ "naId" :: pc \ "title" :: Nil
    } yield pcId.text

    if (parentRecordGroupIds.nonEmpty) parentRecordGroupIds else parentCollectionIds
  }

  def extractContributor(xml: NodeSeq): Seq[String] = {

    //TODO: not handling multiple display values. haven't found example yet.

    val organizationalContributors = for {
      org <- xml \\ "organizationalContributorArray" \ "organizationalContributor"
      name = (org \ "contributor" \ "termName").text
      _type = (org \ "contributorType" \ "termName").text
      if _type != "Publisher"
    } yield name

    val personalContributors = for {
      person <- xml \\ "personalContributorArray" \ "personalContributor"
      name = (person \ "contributor" \ "termName").text
    //_type = (person \ "contributorType" \ "TermName").text
    } yield name

    organizationalContributors ++ personalContributors
  }

  def extractCreator(xml: NodeSeq): Seq[String] = {

    //TODO: not handling multiple display values. haven't found example yet.
    val organizationalCreators = for (
      name <- xml \\ "creatingOrganizationArray" \ "creatingOrganization" \ "creator" \ "termName"
    ) yield name.text

    val individualCreators = for (
      name <- xml \\ "creatingIndividualArray" \ "creatingIndividual" \ "creator" \ "termName"
    ) yield name.text

    if (organizationalCreators.nonEmpty) organizationalCreators else individualCreators
  }

  def extractDate(xml: NodeSeq): Seq[EdmTimeSpan] = {

    val coverageDates = for {
      coverageDate <- xml \\ "coverageDates"
      coverageStartDate = (coverageDate \ "coverageStartDate").headOption
      coverageEndDate = (coverageDate \ "coverageEndDate").headOption
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

    val copyrightDates = simpleDate(xml \\ "copyrightDateArray" \ "proposableQualifiableDate")
    val productionDates = simpleDate(xml \\ "productionDateArray" \ "proposableQualifiableDate")

    //todo not sure what to do with logicalDate
    val broadcastDates = simpleDate(xml \\ "broadcastDateArray" \ "proposableQualifiableDate")
    val releaseDates = simpleDate(xml \\ "releaseDateArray" \ "proposableQualifiableDate")

    Seq(
      coverageDates,
      copyrightDates,
      productionDates,
      broadcastDates,
      releaseDates
      //get the first non-empty one, or an empty one if they're all empty
    ).find(_.nonEmpty).getOrElse(Seq())
  }

  def getDisplayDate(start: Option[Node], end: Option[Node]): Option[String] = {
    if (start.isEmpty && end.isEmpty) {
      None
    } else {
      val startString = start.map(_.text).getOrElse("unknown")
      val endString = end.map(_.text).getOrElse("unknown")
      Some(s"$startString/$endString")
    }
  }

  def simpleDate(nodeSeq: NodeSeq): Seq[EdmTimeSpan] =
    nodeSeq.map(node => EdmTimeSpan(originalSourceDate = nodeToDateString(Some(node))))

  def nodeToDateString(nodeOption: Option[Node]): Option[String] = nodeOption match {
    case None => None
    case Some(node) =>
      val year = (node \ "year").text
      val month = (node \ "month").text
      val day = (node \ "day").text

      (year, month, day) match {
        case (y, m, d) if y.isEmpty => None
        case (y, m, d) if m.isEmpty => Some(year)
        case (y, m, d) if d.isEmpty => Some(s"$year-$month")
        case (y, m, d) => Some(s"$year-$month-$day")
      }
  }

  def extractPublisher(xml: NodeSeq): Seq[String] = {

    val orgs = for {
      org <- xml \\ "organizationalContributorArray" \ "organizationalContributor"
      if (org \ "contributorType" \ "termName").text == "Publisher"
      name <- org \ "termName"
    } yield name.text

    val persons = for {
      person <- xml \\ "personalContributorArray" \ "personalContributor"
      if (person \ "contributorType" \ "termName").text == "Publisher"
      name <- person \ "termName"
    } yield name.text

    orgs ++ persons
  }

  def extractRelation(xml: NodeSeq): Seq[String] = for {
    parentFileUnit <- xml \\ "parentFileUnit"
    value1 = (parentFileUnit \ "title").text
    value2 = (parentFileUnit \ "parentSeries" \ "title").text
    value3a = (parentFileUnit \ "parentRecordGroup" \ "title").text
    value3b = (parentFileUnit \ "parentCollection" \ "title").text
  } yield {
    val value3 = if (value3a.isEmpty) value3b else value3a
    //VALUE3"; "VALUE2"; "VALUE1
    f"""$value3"; $value2"; "$value1""""
  }

  def extractRights(xml: NodeSeq): Seq[String] = for {
    useRestriction <- xml \ "useRestriction"
    value1 = (useRestriction \ "note").text
    value2 = (useRestriction \ "specificUseRestrictionArray" \ "specificUseRestriction" \ "termName").text
    value3 = (useRestriction \ "status" \ "termName").text
  //VALUE2": "VALUE1" "VALUE3
  } yield
    f"""$value2": "$value1" "$value3"""

  def extractTypes(xml: NodeSeq): Seq[String] = for {
    stringType <- extractStrings(xml \\ "generalRecordsTypeArray" \ "generalRecordsType" \ "termName")
    mappedType <- NaraTypeVocabEnforcer.mapNaraType(stringType)
  } yield {
    mappedType.getLocalName.toLowerCase
  }


}

object NaraTypeVocabEnforcer extends VocabEnforcer[String] {
  val dcmiTypes = DCMIType()
  val naraVocab: Map[String, IRI] = Map(
    "Architectural and Engineering Drawings" -> dcmiTypes.Image,
    "Artifacts" -> dcmiTypes.PhysicalObject,
    "Data Files" -> dcmiTypes.Dataset,
    "Maps and Charts" -> dcmiTypes.Image,
    "Moving Images" -> dcmiTypes.MovingImage,
    "Photographs and Other Graphic Materials" -> dcmiTypes.Image,
    "Sound Recordings" -> dcmiTypes.Sound,
    "Textual Records" -> dcmiTypes.Text,
    "Web Pages" -> dcmiTypes.InteractiveResource
  ) ++ DcmiTypeMapper.DcmiTypeMap

  def mapNaraType(value: String): Option[IRI] = mapVocab(value, naraVocab)

}
