package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.TypeEnrichment
import dpla.ingestion3.mappers.rdf.DCMIType
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.eclipse.rdf4j.model.IRI
import org.json4s.JsonAST
import org.json4s.JsonDSL._

import scala.xml.{Node, NodeSeq}

class NaraMapping extends XmlMapping with XmlExtractor {

  private val uriBase = "https://catalog.archives.gov/id/"

  private def uri(naid: String): URI = URI(s"$uriBase$naid")

  // Why is this here?
  override val enforceDuplicateIds: Boolean = false
  override val enforceEdmRights: Boolean =
    true // edmRights is now a required property for NARA

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("nara")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString("naId")(data)

  def itemUri(implicit data: Document[NodeSeq]): URI =
    extractString("naId")(data)
      .map(naId => URI("http://catalog.archives.gov/id/" + naId))
      .getOrElse(throw MappingException("Couldn't load item url."))

  // OreAggregation
  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    val referenceUnit = (for {
      physicalOccurrenceArray <- data \ "physicalOccurrenceArray"
      copyStatus = (physicalOccurrenceArray \\ "copyStatus" \ "termName").text
      // todo Preservation-Reproduction-Reference
      if copyStatus.contains("Reproduction-Reference") || copyStatus.contains(
        "Preservation"
      )
      referenceUnit = (physicalOccurrenceArray \\ "referenceUnit" \ "termName")
        .map(_.text)
        .headOption
    } yield referenceUnit).headOption.flatten

    Seq(
      nameOnlyAgent(
        referenceUnit.getOrElse("National Archives and Records Administration")
      )
    )
  }

  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  /** <useRestriction> \ <status> \ <termName>Unrestricted</termName>
    *
    * LCDRG Use Restriction Status | LCDRG Specific Use Restriction | Rights
    * Statement URL
    * ------------------------------------------------------------------------------------------------------------------------
    * Restricted - Fully | Copyright |
    * http://rightsstatements.org/vocab/InC/1.0/ Restricted - Fully | Donor
    * Restrictions | http://rightsstatements.org/vocab/InC/1.0/ Restricted -
    * Fully | Public Law 101-246 | http://rightsstatements.org/vocab/InC/1.0/
    * Restricted - Fully | Service Mark |
    * http://rightsstatements.org/vocab/NoC-OKLR/1.0/ Restricted - Fully |
    * Trademark | http://rightsstatements.org/vocab/NoC-OKLR/1.0/ Restricted -
    * Fully | Other | http://rightsstatements.org/vocab/NoC-OKLR/1.0/ Restricted
    * \- Partly | Copyright | http://rightsstatements.org/vocab/InC/1.0/
    * Restricted - Partly | Donor Restrictions |
    * http://rightsstatements.org/vocab/InC/1.0/ Restricted - Partly | Public
    * Law 101-246 | http://rightsstatements.org/vocab/InC/1.0/ Restricted -
    * Partly | Service Mark | http://rightsstatements.org/vocab/NoC-OKLR/1.0/
    * Restricted - Partly | Trademark |
    * http://rightsstatements.org/vocab/NoC-OKLR/1.0/ Restricted - Partly |
    * Other | http://rightsstatements.org/vocab/NoC-OKLR/1.0/ Restricted -
    * Possibly | Copyright | http://rightsstatements.org/vocab/UND/1.0/
    * Restricted - Possibly | Donor Restrictions |
    * http://rightsstatements.org/vocab/UND/1.0/ Restricted - Possibly | Public
    * Law 101-246 | http://rightsstatements.org/vocab/NKC/1.0/ Restricted -
    * Possibly | Service Mark | http://rightsstatements.org/vocab/NoC-OKLR/1.0/
    * Restricted - Possibly | Trademark |
    * http://rightsstatements.org/vocab/NoC-OKLR/1.0/ Restricted - Possibly |
    * Other | http://rightsstatements.org/vocab/NoC-OKLR/1.0/ Restricted -
    * Possibly | | http://rightsstatements.org/vocab/UND/1.0/ Undetermined | |
    * http://rightsstatements.org/vocab/CNE/1.0/ Unrestricted | |
    * http://rightsstatements.org/vocab/NoC-US/1.0/
    *
    * @param data
    * @return
    */
  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] = {
    // There can be only one useRestriction
    val useRestriction = extractString(
      data \\ "useRestriction" \ "status" \ "termName"
    )
    // There can be multiple specificUseRestrictions
    val specificRestrictions = for {
      sra <-
        data \\ "useRestriction" \ "specificUseRestrictionArray" \ "specificUseRestriction"
      sr <- extractString(sra \ "termName")
    } yield Option(sr)

    // Standard formulation for rightsstatments.org URI
    val rightsUri = (value: String) =>
      URI(s"http://rightsstatements.org/vocab/$value/1.0/")

    val edmRights = List(useRestriction)
      .zipAll(
        specificRestrictions,
        None,
        None
      ) // merge useRestriction and specificRestriction into set,
      .map { case (ur: Option[String], sr: Option[String]) =>
        (ur, sr) match {
          case (Some("Restricted - Fully"), Some("Copyright")) =>
            Some(rightsUri("InC"))
          case (Some("Restricted - Fully"), Some("Donor Restrictions")) =>
            Some(rightsUri("InC"))
          case (Some("Restricted - Fully"), Some("Public Law 101-246")) =>
            Some(rightsUri("InC"))
          case (Some("Restricted - Fully"), Some("Service Mark")) =>
            Some(rightsUri("NoC-OKLR"))
          case (Some("Restricted - Fully"), Some("Trademark")) =>
            Some(rightsUri("NoC-OKLR"))
          case (Some("Restricted - Fully"), Some("Other")) =>
            Some(rightsUri("NoC-OKLR"))
          case (Some("Restricted - Partly"), Some("Copyright")) =>
            Some(rightsUri("InC"))
          case (Some("Restricted - Partly"), Some("Donor Restrictions")) =>
            Some(rightsUri("InC"))
          case (Some("Restricted - Partly"), Some("Public Law 101-246")) =>
            Some(rightsUri("InC"))
          case (Some("Restricted - Partly"), Some("Service Mark")) =>
            Some(rightsUri("NoC-OKLR"))
          case (Some("Restricted - Partly"), Some("Trademark")) =>
            Some(rightsUri("NoC-OKLR"))
          case (Some("Restricted - Partly"), Some("Other")) =>
            Some(rightsUri("NoC-OKLR"))
          case (Some("Restricted - Possibly"), Some("Copyright")) =>
            Some(rightsUri("UND"))
          case (Some("Restricted - Possibly"), Some("Donor Restrictions")) =>
            Some(rightsUri("UND"))
          case (Some("Restricted - Possibly"), Some("Public Law 101-246")) =>
            Some(rightsUri("NKC"))
          case (Some("Restricted - Possibly"), Some("Service Mark")) =>
            Some(rightsUri("NoC-OKLR"))
          case (Some("Restricted - Possibly"), Some("Trademark")) =>
            Some(rightsUri("NoC-OKLR"))
          case (Some("Restricted - Possibly"), Some("Other")) =>
            Some(rightsUri("NoC-OKLR"))
          case (Some("Restricted - Possibly"), _) => Some(rightsUri("UND"))
          case (Some("Undetermined"), _)          => Some(rightsUri("CNE"))
          case (Some("Unrestricted"), _)          => Some(rightsUri("NoC-US"))
          case (_, _)                             => None
        }
      }

    // The most restrictive statements
    val mostRestrictive = Seq(
      Some(rightsUri("InC")), // in copyright
      Some(rightsUri("UND")) // copyright undetermined
    )

    // If more than one rights statement is mapped then select either of the most restrictive statements
    // This conforms to the expectation that there will ever be specific combinations of Use + Specific Restrictions
    //
    // Example:
    // A "Restricted - Fully" Use Restriction could produce both `InC` AND `NoC-OKLR` and therefore `InC`
    // should be selected rather than returning both values and dropping one at random (see Mapper.validateEdmRights())
    //
    // Restricted - Fully
    //    InC and NoC-OKLR = InC
    //
    // Restricted - Partly
    //    InC and NoC-OKLR = InC
    //
    // Restricted - Possibly
    //    UND and NKC and NoC-OKLR = UND
    //
    // Restricted - Possibly
    //    NKC and NoC-OKLR = No rights value will be mapped
    //
    // If any other combination of values exists the behavior is uncertain. Ex. if a record contains both UND and InC values
    // both values will be mapped and the first value will be selected from the behavior in `validateEdmRights()` but a
    // warning for multiple edmRights values will also be recorded in the logs.
    if (edmRights.size > 1)
      edmRights.intersect(mostRestrictive).flatten
    else
      edmRights.flatten
  }

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    Seq(uriOnlyWebResource(itemUri(data)))

  override def mediaMaster(
      data: Document[NodeSeq]
  ): ZeroToMany[EdmWebResource] = for {
    digitalObject <- data \ "digitalObjectArray" \ "digitalObject"
    accessFileName = (digitalObject \ "accessFilename").text.trim
    badPrefix = "https://opaexport-conv.s3.amazonaws.com/"
    url =
      if (accessFileName.startsWith(badPrefix)) {
        originalId(data) match {
          case Some(id) =>
            s"https://catalog.archives.gov/OpaAPI/media/$id/content/${accessFileName.stripPrefix(badPrefix)}"
          case None => null
        }
      } else {
        accessFileName
      }.replaceFirst("https/", "https:/") // some urls are missing the colon for some reason.
  } yield stringOnlyWebResource(url)

  override def `object`(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractPreview(data)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] =
    agent

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    extractPreview(data)

  override def sidecar(data: Document[NodeSeq]): JsonAST.JValue =
    ("prehashId", buildProviderBaseId()(data)) ~ ("dplaId", mintDplaId(data))

  // SourceResource
  override def collection(data: Document[NodeSeq]): Seq[DcmiTypeCollection] =
    extractCollection(data)

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
    extractStrings(data \\ "languageArray" \ "language" \ "termName")
      .map(nameOnlyConcept)

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    extractStrings(
      data \\ "geographicReferenceArray" \ "geographicPlaceName" \ "termName"
    ).map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    extractPublisher(data).map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    extractRelation(data).map(LiteralOrUri(_, isUri = false))

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    extractRights(data)

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    (data \\ "topicalSubjectArray" \ "topicalSubject").map(node => {
      val name = extractString(node \ "termName")
      val subjectUri = extractStrings(node \ "naId").map(uri)
      SkosConcept(providedLabel = name, exactMatch = subjectUri)
    })

  override def title(data: Document[NodeSeq]): AtLeastOne[String] =
    extractStrings("title")(data)

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractTypes(data)

  // Helper methods
  private def agent = EdmAgent(
    name = Some("National Archives and Records Administration"),
    uri = Some(URI("http://dp.la/api/contributor/nara"))
  )

  private def extractCollection(data: NodeSeq): Seq[DcmiTypeCollection] = {
    val parentRecordGroupIds = for {
      prg <- data \\ "parentRecordGroup" \\ "title"
      prgUri <- extractString(data \\ "parentRecordGroup" \\ "naId")
        .map(naId => s"$uriBase$naId")
        .map(stringOnlyWebResource)
    } yield DcmiTypeCollection(
      title = Option(prg.text),
      isShownAt = Some(prgUri)
    )

    val parentCollectionIds = for {
      pc <- data \\ "parentCollection" \ "title"
      pcUri <- extractString(data \\ "parentCollection" \ "naId")
        .map(naId => s"$uriBase$naId")
        .map(stringOnlyWebResource)
    } yield DcmiTypeCollection(title = Option(pc.text), isShownAt = Some(pcUri))

    val parentSeries = for {
      ps <- data \\ "parentSeries" \ "title"
      psUri <- extractString(data \\ "parentSeries" \ "naId")
        .map(naId => s"$uriBase$naId")
        .map(stringOnlyWebResource)
    } yield DcmiTypeCollection(title = Option(ps.text), isShownAt = Some(psUri))

    val parentFileUnit = for {
      pfu <- data \\ "parentFileUnit" \ "title"
      pfuUri <- extractString(data \\ "parentFileUnit" \ "naId")
        .map(naId => s"$uriBase$naId")
        .map(stringOnlyWebResource)
    } yield DcmiTypeCollection(
      title = Option(pfu.text),
      isShownAt = Some(pfuUri)
    )

    if (parentRecordGroupIds.nonEmpty)
      parentRecordGroupIds ++ parentSeries ++ parentFileUnit
    else
      parentCollectionIds ++ parentSeries ++ parentFileUnit
  }

  private def extractContributor(data: NodeSeq): Seq[String] = {

    // TODO: not handling multiple display values. haven't found example yet.

    val organizationalContributors = for {
      org <-
        data \\ "organizationalContributorArray" \ "organizationalContributor"
      name = (org \ "contributor" \ "termName").text
      _type = (org \ "contributorType" \ "termName").text
      if !_type.contains("Publisher")
    } yield name

    val personalContributors = for {
      person <- data \\ "personalContributorArray" \ "personalContributor"
      name = (person \ "contributor" \ "termName").text
      // _type = (person \ "contributorType" \ "TermName").text
    } yield name

    organizationalContributors ++ personalContributors
  }

  private def extractCreator(data: NodeSeq): Seq[String] = {
    // TODO: not handling multiple display values. haven't found example yet.
    val organizationalCreators =
      for {
        creatingOrganization <-
          data \\ "creatingOrganizationArray" \ "creatingOrganization"
        creator <- (creatingOrganization \ "creator" \ "termName").headOption
          .map(_.text)
        creatorType =
          (creatingOrganization \ "creatorType" \ "termName").headOption
            .map(_.text)
        if creatorType.getOrElse("").contains("Most Recent")
      } yield creator

    val individualCreators = for {
      creator <-
        data \\ "creatingIndividualArray" \ "creatingIndividual" \ "creator" \ "termName"
    } yield creator.text

    if (organizationalCreators.nonEmpty)
      organizationalCreators
    else
      individualCreators
  }

  private def extractDate(data: NodeSeq): Seq[EdmTimeSpan] = {

    val coverageDates = for {
      coverageDate <- data \\ "coverageDates"
      coverageStartDate =
        (coverageDate \ "coverageStartDate" \ "logicalDate").headOption
      coverageEndDate =
        (coverageDate \ "coverageEndDate" \ "logicalDate").headOption
      // dateQualifier = coverageDate \ "dateQualifier" //todo not sure what to do with qualifier in EDTF
      if coverageStartDate.nonEmpty || coverageEndDate.nonEmpty
    } yield {
      val displayDate = getDisplayDate(coverageStartDate, coverageEndDate)
      EdmTimeSpan(
        originalSourceDate = displayDate, // this is cheating?
        begin = nodeToDateString(coverageStartDate),
        end = nodeToDateString(coverageEndDate),
        prefLabel = displayDate
      )
    }

    val copyrightDates = simpleDate(
      data \\ "copyrightDateArray" \ "proposableQualifiableDate"
    )
    val productionDates = simpleDate(
      data \\ "productionDateArray" \ "proposableQualifiableDate"
    )

    // todo not sure what to do with logicalDate
    val broadcastDates = simpleDate(
      data \\ "broadcastDateArray" \ "proposableQualifiableDate"
    )
    val releaseDates = simpleDate(
      data \\ "releaseDateArray" \ "proposableQualifiableDate"
    )

    val lastResort = for {
      inclusiveDate <-
        data \ "parentFileUnit" \ "parentSeries" \ "inclusiveDates"
      inclusiveStartDate = removeTime(
        (inclusiveDate \ "inclusiveStartDate" \ "logicalDate").text
      )
      inclusiveEndDate = removeTime(
        (inclusiveDate \ "inclusiveEndDate" \ "logicalDate").text
      )
    } yield {
      val startOption = Option(inclusiveStartDate)
      val endOption = Option(inclusiveEndDate)
      val displayDate = Option(
        startOption.getOrElse("unknown") + "/" + endOption.getOrElse("unknown")
      )
      EdmTimeSpan(
        originalSourceDate = displayDate,
        begin = startOption,
        end = endOption
      )
    }

    Seq(
      coverageDates,
      copyrightDates,
      productionDates,
      broadcastDates,
      releaseDates,
      lastResort
      // get the first non-empty one, or an empty one if they're all empty
    ).find(_.nonEmpty).getOrElse(Seq())
  }

  private def extractFormat(data: NodeSeq): Seq[String] =
    (extractStrings(
      data \\ "specificRecordsTypeArray" \\ "specificRecordsType" \ "termName"
    ) ++
      extractStrings(
        data \\ "mediaOccurrenceArray" \\ "specificMediaType" \ "termName"
      ) ++
      extractStrings(data \\ "mediaOccurrenceArray" \\ "color" \ "termName") ++
      extractStrings(
        data \\ "mediaOccurrenceArray" \\ "dimensions" \ "termName"
      ) ++
      extractStrings(
        data \\ "mediaOccurrenceArray" \\ "generalMediaType" \ "termName"
      )).distinct

  /** removes the time portion of an ISO-8601 datetime
    *
    * @param string date string
    * @return
    *   string without the time, if there was one
    */
  private def removeTime(string: String): String = {
    if (string.contains("T")) string.substring(0, string.indexOf('T'))
    else string
  }

  private def getDisplayDate(start: Option[Node], end: Option[Node]): Option[String] = {
    if (start.isEmpty && end.isEmpty) {
      None
    } else {
      val startString = start.map(x => removeTime(x.text)).getOrElse("unknown")
      val endString = end.map(x => removeTime(x.text)).getOrElse("unknown")
      Some(s"$startString/$endString")
    }
  }

  private def simpleDate(nodeSeq: NodeSeq): Seq[EdmTimeSpan] =
    nodeSeq.map(node =>
      EdmTimeSpan(originalSourceDate = nodeToDateString(Some(node)))
    )

  private def lpad(string: String, digits: Int): String = {
    val trimString = string.trim
    if (trimString.isEmpty) string
    else if (!trimString.forall(Character.isDigit)) trimString
    else ("%0" + digits + "d").format(trimString.toInt)
  }

  private def nodeToDateString(nodeOption: Option[Node]): Option[String] =
    nodeOption match {
      case None => None
      case Some(node) =>
        val year = lpad((node \ "year").text, 4)
        val month = lpad((node \ "month").text, 2)
        val day = lpad((node \ "day").text, 2)

        (year, month, day) match {
          case (y, _, _) if y.isEmpty => None
          case (y, m, _) if m.isEmpty => Some(y)
          case (y, m, d) if d.isEmpty => Some(s"$y-$m")
          case (y, m, d)              => Some(s"$y-$m-$d")
        }
    }

  private def extractPreview(data: Document[NodeSeq]): Seq[EdmWebResource] =
    for {
      digitalObject <- data \ "digitalObjectArray" \ "digitalObject"
      accessFileName = (digitalObject \ "accessFilename").text.trim
      termName = (digitalObject \ "objectType" \ "termName").text.toLowerCase

      badPrefix = "https://opaexport-conv.s3.amazonaws.com/"
      url = if (accessFileName.startsWith(badPrefix)) {
        originalId(data) match {
          case Some(id) =>
            "https://catalog.archives.gov/OpaAPI/media/" +
              id + "/content/" + accessFileName.stripPrefix(badPrefix)
          case None => null
        }
      } else {
        accessFileName
      }.replaceFirst("https/", "https:/") // some urls are missing the colon for some reason.
      if termName.contains("image") &&
        (termName.contains("jpg") || termName.contains("gif")) &&
        url != null

    } yield stringOnlyWebResource(url)

  private def extractPublisher(data: NodeSeq): Seq[String] = {

    val orgs = for {
      org <-
        data \\ "organizationalContributorArray" \ "organizationalContributor"
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

    val mediaTypes =
      for (
        title <-
          data \ "microformPublicationArray" \ "microformPublication" \ "title"
      ) yield title.text

    val parents =
      if (parentFileUnitRelation.nonEmpty) parentFileUnitRelation
      else if (parentSeriesRelation.nonEmpty) parentSeriesRelation
      else Seq()

    (parents ++ mediaTypes).distinct
  }

  private def extractRights(data: NodeSeq): Seq[String] = (for {
    useRestriction <- data \ "useRestriction"
    value1 = (useRestriction \ "note").text
    value2 =
      (useRestriction \ "specificUseRestrictionArray" \ "specificUseRestriction" \ "termName").text
    value3 = (useRestriction \ "status" \ "termName").text
  } yield Seq(value2, value1, value3).filter(_.nonEmpty).mkString(" ; "))
    .filter(_.nonEmpty)

  private def extractTypes(data: NodeSeq): Seq[String] = for {
    stringType <- extractStrings(
      data \\ "generalRecordsTypeArray" \ "generalRecordsType" \ "termName"
    )
    mappedType <- NaraTypeVocabEnforcer.mapNaraType(stringType)
  } yield {
    mappedType
  }
}

object NaraTypeVocabEnforcer {
  private val dcmiTypes = DCMIType()
  private val naraVocab: Map[String, IRI] = Map(
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

  private val naraTypeMapper = new TypeEnrichment
  naraTypeMapper.addVocab(naraVocab)

  def mapNaraType(value: String): Option[String] = naraTypeMapper.enrich(value)
}
