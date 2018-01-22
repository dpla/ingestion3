package dpla.ingestion3.mappers.providers

import java.net.URI

import dpla.ingestion3.enrichments.{DcmiTypeMapper, DcmiTypeStringMapper, VocabEnforcer}
import dpla.ingestion3.mappers.rdf.DCMIType
import dpla.ingestion3.mappers.xml.XmlExtractionUtils
import dpla.ingestion3.model._
import org.eclipse.rdf4j.model.IRI
import org.json4s.JsonDSL._

import scala.util.Try
import scala.xml.{Node, NodeSeq, XML}


class NaraExtractor(rawData: String, shortName: String) extends Extractor with XmlExtractionUtils with Serializable {

  implicit val xml: NodeSeq = XML.loadString(rawData)

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = shortName

  // itemUri will throw an exception if an ID is missing
  override def getProviderId(): String = extractString("naId").getOrElse(throw ExtractorException("Can't find naId"))

  def itemUri(implicit xml: NodeSeq): URI =
    extractString("naId").map(naId => new URI("http://catalog.archives.gov/id/" + naId))
      .getOrElse(throw ExtractorException("Couldn't load item url."))

  override def build(): Try[OreAggregation] = {
    Try {
      OreAggregation(
        dplaUri = mintDplaItemUri(),
        sidecar = ("prehashId", buildProviderBaseId()) ~ ("dplaId", mintDplaId()),
        sourceResource = DplaSourceResource(
          collection = collection(xml).map(nameOnlyCollection),
          contributor = contributor(xml).map(nameOnlyAgent),
          creator = creator(xml).map(nameOnlyAgent),
          date = date(xml),
          description = extractStrings("scopeAndContentNote"),
          extent = extractStrings(xml \ "physicalOccurrenceArray" \\ "extent"),
          format = format(xml),
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
        preview = preview(xml).headOption
      )
    }
  }

  def preview(xml: NodeSeq): Seq[EdmWebResource] = for {
    digitalObject <- xml \ "digitalObjectArray" \ "digitalObject"
    accessFileName = (digitalObject \ "accessFilename").text
    termName = (digitalObject \ "objectType" \ "termName").text
    if termName.contains("Image") && termName.contains("JPG")
  } yield uriOnlyWebResource(new URI(accessFileName.trim))

  def agent = EdmAgent(
    name = Some("National Archives and Records Administration"),
    uri = Some(new URI("http://dp.la/api/contributor/nara"))
  )

  def format(xml: NodeSeq): Seq[String] =
    (extractStrings(xml \\ "specificRecordsTypeArray" \\ "specificRecordsType" \ "termName") ++
      extractStrings(xml \\ "mediaOccurrenceArray" \\ "specificMediaType" \ "termName") ++
      extractStrings(xml \\ "mediaOccurrenceArray" \\ "color" \ "termName") ++
      extractStrings(xml \\ "mediaOccurrenceArray" \\ "dimensions" \ "termName") ++
      extractStrings(xml \\ "mediaOccurrenceArray" \\ "generalMediaType" \ "termName")).distinct

  def collection(xml: NodeSeq): Seq[String] = {
    val parentRecordGroupIds = for {
      prg <- xml \\ "parentRecordGroup" \\ "title"
    } yield prg.text

    val parentCollectionIds = for {
      pc <- xml \\ "parentCollection" \ "title"
    } yield pc.text

    if (parentRecordGroupIds.nonEmpty) parentRecordGroupIds else parentCollectionIds
  }

  def contributor(xml: NodeSeq): Seq[String] = {

    //TODO: not handling multiple display values. haven't found example yet.

    val organizationalContributors = for {
      org <- xml \\ "organizationalContributorArray" \ "organizationalContributor"
      name = (org \ "contributor" \ "termName").text
      _type = (org \ "contributorType" \ "termName").text
      if !_type.contains("Publisher")
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
    val organizationalCreators =
      for {
        creatingOrganization <- xml \\ "creatingOrganizationArray" \ "creatingOrganization"
        creator <- (creatingOrganization \ "creator" \ "termName").headOption.map(_.text)
        creatorType = (creatingOrganization \ "creatorType" \ "termName").headOption.map(_.text)
        if creatorType.getOrElse("").contains("Most Recent")
      } yield creator

    val individualCreators = for {
      creator <- xml \\ "creatingIndividualArray" \ "creatingIndividual" \ "creator" \ "termName"
    } yield creator.text

    if (organizationalCreators.nonEmpty)
      organizationalCreators
    else
      individualCreators
  }

  def date(xml: NodeSeq): Seq[EdmTimeSpan] = {

    val coverageDates = for {
      coverageDate <- xml \\ "coverageDates"
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

    val copyrightDates = simpleDate(xml \\ "copyrightDateArray" \ "proposableQualifiableDate")
    val productionDates = simpleDate(xml \\ "productionDateArray" \ "proposableQualifiableDate")

    //todo not sure what to do with logicalDate
    val broadcastDates = simpleDate(xml \\ "broadcastDateArray" \ "proposableQualifiableDate")
    val releaseDates = simpleDate(xml \\ "releaseDateArray" \ "proposableQualifiableDate")

    val lastResort = for {
      inclusiveDate <- xml \ "parentFileUnit" \ "parentSeries" \ "inclusiveDates"
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

  def relation(xml: NodeSeq): Seq[String] = {

    val parentFileUnitRelation = for {
      parentFileUnit <- xml \\ "parentFileUnit"
      value1 = (parentFileUnit \ "title").text
      value2 = (parentFileUnit \ "parentSeries" \ "title").text
      value3a = (parentFileUnit \ "parentRecordGroup" \ "title").text
      value3b = (parentFileUnit \ "parentCollection" \ "title").text
      value3 = if (value3a.isEmpty) value3b else value3a
    } yield Seq(value3, value2, value1).filter(_.nonEmpty).mkString(" ; ")

    val parentSeriesRelation = for {
      parentSeries <- xml \\ "parentSeries"
      value2 = (parentSeries \ "title").text
      value3a = (parentSeries \ "parentRecordGroup" \ "title").text
      value3b = (parentSeries \ "parentCollection" \ "title").text
      value3 = if (value3a.isEmpty) value3b else value3a
    } yield Seq(value3, value2).filter(_.nonEmpty).mkString(" ; ")

    val mediaTypes = for (
      title <- xml \ "microformPublicationArray" \ "microformPublication" \ "title"
    ) yield title.text

    val parents = if (parentFileUnitRelation.nonEmpty) parentFileUnitRelation
    else if (parentSeriesRelation.nonEmpty) parentSeriesRelation
    else Seq()

    (parents ++ mediaTypes).distinct
  }

  def rights(xml: NodeSeq): Seq[String] = for {
    useRestriction <- xml \ "useRestriction"
    value1 = (useRestriction \ "note").text
    value2 = (useRestriction \ "specificUseRestrictionArray" \ "specificUseRestriction" \ "termName").text
    value3 = (useRestriction \ "status" \ "termName").text
  } yield Seq(value2, value1, value3).filter(_.nonEmpty).mkString(" ; ")


  def types(xml: NodeSeq): Seq[String] = for {
    stringType <- extractStrings(xml \\ "generalRecordsTypeArray" \ "generalRecordsType" \ "termName")
    mappedType <- NaraTypeVocabEnforcer.mapNaraType(stringType)
  } yield {
    mappedType
  }

  def dataProvider(xml: NodeSeq): EdmAgent = {
    val referenceUnit = (for {
      physicalOccurrenceArray <- xml \ "physicalOccurrenceArray"
      copyStatus = (physicalOccurrenceArray \\ "copyStatus" \ "termName").text
      //todo Preservation-Reproduction-Reference
      if copyStatus.contains("Reproduction-Reference") || copyStatus.contains("Preservation")
      referenceUnit = (physicalOccurrenceArray \\ "referenceUnit" \ "termName").text
    } yield referenceUnit).headOption
    nameOnlyAgent(referenceUnit.getOrElse("National Records and Archives Administration"))
  }
}

object NaraTypeVocabEnforcer extends VocabEnforcer[String] {
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
  ) ++ DcmiTypeMapper.DcmiTypeMap

  def mapNaraType(value: String): Option[String] =
    mapVocab(value.toLowerCase, naraVocab)
      .map(foo => DcmiTypeStringMapper.mapDcmiTypeString(foo).toLowerCase)


}
