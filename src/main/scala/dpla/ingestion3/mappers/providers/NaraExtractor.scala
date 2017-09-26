package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.enrichments.{DcmiTypeMapper, VocabEnforcer}
import dpla.ingestion3.mappers.rdf.DCMIType
import dpla.ingestion3.mappers.xml.XmlExtractionUtils
import dpla.ingestion3.model._
import org.eclipse.rdf4j.model.IRI

import scala.util.Try
import scala.xml.{Node, NodeSeq, XML}

class NaraExtractor(rawData: String, shortName: String) extends Extractor with XmlExtractionUtils with Serializable {

  implicit val xml: NodeSeq = XML.loadString(rawData)

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = shortName

  // itemUri will throw an exception if an ID is missing
  override def getProviderId(): String = itemUri.toString

  def itemUri(implicit xml: NodeSeq): URI =
    extractString("naId").map(naId => new URI("http://catalog.archives.gov/id/" + naId))
      .getOrElse(throw ExtractorException("Couldn't load item url."))

  override def build(): Try[OreAggregation] = {
    Try {
      OreAggregation(
        dplaUri = mintDplaItemUri(),
        sourceResource = DplaSourceResource(
          collection = collection(xml).map(nameOnlyCollection),
          contributor = contributor(xml).map(nameOnlyAgent),
          creator = creator(xml).map(nameOnlyAgent),
          date = date(xml),
          description = extractStrings("scopeAndContentNote"),
          extent = extractStrings("extent"),
          format = extractStrings(xml \\ "specificRecordsTypeArray" \ "specificRecordsType" \ "termName"),
          identifier = extractStrings("naId"),
          language = extractStrings(xml \\ "languageArray" \ "language" \ "termName").map(nameOnlyConcept),
          place =
            extractStrings(xml \\ "geographicReferenceArray" \ "geographicPlaceName" \ "termName").map(nameOnlyPlace),
          publisher = publisher(xml).map(nameOnlyAgent),
          relation = relation(xml).map(Left(_)),
          rights = rights(xml),
          subject = extractStrings(xml \\ "topicalSubjectArray" \ "topicalSubject" \ "termName").map(nameOnlyConcept),
          title = extractStrings("title"),
          `type` = types(xml)
        ),
        dataProvider = dataProvider(xml),
        originalRecord = rawData,
        provider = agent,
        isShownAt = uriOnlyWebResource(itemUri(xml)),
        preview = extractString(xml \ "digitalObjectArray" \ "digitalObject" \ "thumbnailFilename").map(new URI(_)).map(uriOnlyWebResource)
      )
    }
  }

  def agent = EdmAgent(
    name = Some("National Archives and Records Administration"),
    uri = Some(new URI("http://dp.la/api/contributor/nara"))
  )

  def collection(xml: NodeSeq): Seq[String] = {
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

  def contributor(xml: NodeSeq): Seq[String] = {

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

  def creator(xml: NodeSeq): Seq[String] = {

    //TODO: not handling multiple display values. haven't found example yet.
    val organizationalCreators = for (
      name <- xml \\ "creatingOrganizationArray" \ "creatingOrganization" \ "creator" \ "termName"
    ) yield name.text

    val individualCreators = for (
      name <- xml \\ "creatingIndividualArray" \ "creatingIndividual" \ "creator" \ "termName"
    ) yield name.text

    if (organizationalCreators.nonEmpty) organizationalCreators else individualCreators
  }

  def date(xml: NodeSeq): Seq[EdmTimeSpan] = {

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

  def publisher(xml: NodeSeq): Seq[String] = {

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

  def relation(xml: NodeSeq): Seq[String] = for {
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

  def rights(xml: NodeSeq): Seq[String] = for {
    useRestriction <- xml \ "useRestriction"
    value1 = (useRestriction \ "note").text
    value2 = (useRestriction \ "specificUseRestrictionArray" \ "specificUseRestriction" \ "termName").text
    value3 = (useRestriction \ "status" \ "termName").text
  //VALUE2": "VALUE1" "VALUE3
  } yield
    f"""$value2": "$value1" "$value3"""

  def types(xml: NodeSeq): Seq[String] = for {
    stringType <- extractStrings(xml \\ "generalRecordsTypeArray" \ "generalRecordsType" \ "termName")
    mappedType <- NaraTypeVocabEnforcer.mapNaraType(stringType)
  } yield {
    mappedType.getLocalName.toLowerCase
  }

  def dataProvider(xml: NodeSeq): EdmAgent = {
    val referenceUnit = (for {
      itemPhysicalOccurrence <- xml \ "physicalOccurrenceArray" \ "itemPhysicalOccurrence"
      copyStatus = (itemPhysicalOccurrence \ "copyStatus" \ "termName").text
      if copyStatus == "Reproduction-Reference" || copyStatus == "Preservation"
      referenceUnit = (itemPhysicalOccurrence \ "referenceUnit" \ "termName").text
    } yield referenceUnit).headOption

    nameOnlyAgent(referenceUnit.getOrElse("National Records and Archives Administration"))
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




