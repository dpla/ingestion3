package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.filters.{DigitalSurrogateBlockList, FormatTypeValuesBlockList}
import dpla.ingestion3.mappers.utils.{Document, XmlExtractor, XmlMapping}
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml._

class NYPLMapping extends XmlMapping with XmlExtractor with IngestMessageTemplates {

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  val latLongRegex = "^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?),\\s*[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$"

  val creatorRoles = Seq("architect", "art copyist", "artist", "attributed name", "author","binder","binding designer",
    "book jacket designer","bookplate designer","calligrapher","cartographer","choreographer","composer","correspondent",
    "costume designer","cover designer","creator","dedicator","designer","director","dissertant","dubious author",
    "engraver","etcher","facsimilist","film director","forger","illustrator","interviewee","interviewer","inventor",
    "landscape architect","librettist","lighting designer","lithographer","lyricist","papermaker","performer",
    "photographer","sculptor","set designer","singer","sound designer","wood engraver","woodcutter")

  val contributorRoles = Seq("actor", "adapter","addressee","analyst","animator","annotator","applicant","arranger",
    "art director","artistic director", "assignee","associated name","auctioneer","author in quotations or text extracts",
    "author of afterword, colophon, etc.", "author of dialog","author of introduction, etc.","bibliographic antecedent",
    "blurbwriter","book designer", "book producer", "bookseller","censor","cinematographer","client","collector",
    "collotyper","colorist","commentator","commentator for written text","compiler","complainant","complainant-appellant",
    "complainant-appellee","compositor","conceptor","conductor", "conservator","consultant","consultant to a project",
    "contestant","contestant-appellant","contestant-appellee","contestee", "contestee-appellant","contestee-appellee",
    "contractor","contributor","copyright claimant","copyright holder","corrector",
    "curator","curator of an exhibition","dancer","data contributor","data manager","dedicatee","defendant","defendant-appellant",
    "defendant-appellee","degree granting institution","degree grantor","delineator","depositor","distributor","donor",
    "draftsman","editor","editor of compilation","editor of moving image work","electrician","electrotyper","engineer","expert",
    "field director","film distributor","film editor","film producer","first party","former owner","funder","geographic information specialist","honoree","host","illuminator","inscriber","instrumentalist","issuing body","judge","laboratory",
    "laboratory director","lead","lender","libelant","libelant-appellant","libelant-appellee","libelee","libelee-appellant",
    "libelee-appellee","licensee","licensor","manufacturer","marbler","markup editor","metadata contact","metal-engraver",
    "moderator","monitor","music copyist","musical director","musician","narrator","opponent","organizer","originator",
    "other","owner","panelist","patent applicant","patent holder","patron","permitting agency","plaintiff","plaintiff-appellant",
    "plaintiff-appellee","platemaker","presenter","printer","printer of plates","printmaker","process contact","producer",
    "production company","production manager","production personnel","programmer","project director","proofreader",
    "publisher","publishing director","puppeteer","radio producer","recording engineer","redaktor","fenderer","feporter",
    "research team head","research team member","researcher","respondent","respondent-appellant","respondent-appellee",
    "responsible party","restager","reviewer","rubricator","scenarist","scientific advisor","screenwriter","scribe",
    "second party","secretary","signer","speaker","sponsor","stage director","stage manager","standards body","stereotyper",
    "storyteller","supporting host","surveyor","teacher","technical director","television director","television producer",
    "thesis advisor","transcriber","translator","type designer","typographer","videographer","voice actor","witness",
    "writer of accompanying material")

  // ID minting functions
  override def useProviderName(): Boolean = true

  override def getProviderName(): String = "nypl"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "identifier")
      .map(_.trim)

  override def title(data: Document[NodeSeq]): Seq[String] =
      // titleInfo \ "title" @usage='primary'
      (data \ "titleInfo")
        .flatMap(node => getByAttribute(node, "usage", "primary"))
        .flatMap(node => extractStrings (node \ "title"))

  override def alternateTitle(data: Document[NodeSeq]): ZeroToMany[String] =
    // all other title values
    extractStrings(data \ "titleInfo" \ "title")
      .diff(title(data))

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] = {
    val types = Seq("local_imageid", "isbn", "isrc", "isan", "ismn", "iswc", "issn", "uri", "urn")

    (data \ "identifier")
      .filter(node => filterAttributeListOptions(node, "type", types) || filterAttributeListOptions(node, "displayLabel", types))
      .flatMap(extractStrings)
  }

  override def description(data: Document[NodeSeq]): ZeroToMany[String] = {
    // note @type='content'
    // abstract
    (data \ "note")
      .flatMap(node => getByAttribute(node, "type", "content"))
      .flatMap(extractStrings) ++
    extractStrings(data \ "abstract")
  }

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
  // For the base URL was https://digitalcollections.nypl.org/items/
  // plus
  // uuid 4d0e0bc0-c540-012f-1857-58d385a7bc34
  // twas ever thus
  // https://digitalcollections.nypl.org/items/4d0e0bc0-c540-012f-1857-58d385a7bc34
    (data \ "identifier")
      .flatMap(node => getByAttribute(node, "type", "uuid"))
      .flatMap(extractStrings)
      .map(uuid => s"https://digitalcollections.nypl.org/items/$uuid")
      .map(stringOnlyWebResource)

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    val subjectKeys = Seq("topic", "geographic", "temporal", "occupation")

    val subjectTitles = extractStrings(data \ "subject" \ "titleInfo" \ "title")
    val subjectNames = extractStrings(data \ "subject" \ "name" \ "namePart")
    val subjects = subjectKeys
      .map(key => data \ "subject" \ key)
      .flatMap(extractStrings)

    (subjects ++ subjectNames ++ subjectTitles).map(nameOnlyConcept)
  }

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "typeOfResource")

  override def format(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "physicalDescription" \ "format") ++
    extractStrings(data \ "genre")

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    extractStrings(data \ "physicalDescription" \ "extent")

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    extractStrings(data \ "subject" \ "temporal").map(stringOnlyTimeSpan)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = agentHelper(data, creatorRoles)

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = agentHelper(data, contributorRoles)

  override def collection(data: Document[NodeSeq]): ZeroToMany[DcmiTypeCollection] = ???
    // This mapping seems overly complicated

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = ???
    // this is complicated

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = ???
  // this is complicated

  override def edmRights(data: Document[NodeSeq]): ZeroToMany[URI] = ???
    // rights information appears to exist outside the metadata record an inside the JSON block

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] = ???
    // same problem as edmRights

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    extractStrings(data \ "subject" \ "geographic").map(nameOnlyPlace)

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    extractStrings(data \ "language" \ "languageTerm").map(nameOnlyConcept)

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    (data \ "location" \ "physicalLocation")
      .filterNot(node => filterAttribute(node, "authority", "marcorg")) // FIXME, this filter isn't working as expected
      .map(node => getByAttribute(node, "type", "division"))
      .flatMap(extractStrings)
      .map(_.stripSuffix(".").trim)
      .map(dataProvider => s"$dataProvider. The New York Public Library")
      .distinct
      .map(nameOnlyAgent)
  }

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("New York Public Library"),
    uri = Some(URI("http://dp.la/api/contributor/nypl"))
  )

  def agentHelper(data: Document[NodeSeq], roles: Seq[String]): ZeroToMany[EdmAgent] =
    (data \ "name")
      .filter(node =>
        extractStrings(node \ "role" \ "roleTerm")
          .map(_.toLowerCase.stripSuffix(" ."))
          .exists(roles.contains(_)))
      .flatMap(node => extractStrings(node \ "namePart"))
      .map(nameOnlyAgent)
}
