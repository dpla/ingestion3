package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.filters.{
  DigitalSurrogateBlockList,
  FormatTypeValuesBlockList
}
import dpla.ingestion3.mappers.utils
import dpla.ingestion3.mappers.utils._
import dpla.ingestion3.messages.IngestMessageTemplates
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model.{EdmWebResource, _}
import dpla.ingestion3.utils.Utils
import org.json4s
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.xml.{Elem, NodeSeq, XML}

class NyplJsonExtractor extends JsonExtractor

class NyplXmlExtractor extends XmlExtractor

/*
This is a weird mapper because NYPL now supplies us with a Solr document that has
a MODS XML string embedded in it. The ThreadLocal field stores the parsed MODS XML
so we're not reparsing it for every field we need to extract.
*/

class NyplMapping extends JsonMapping with IngestMessageTemplates {

  // extractors
  lazy val json: NyplJsonExtractor = new NyplJsonExtractor
  lazy val xml: NyplXmlExtractor = new NyplXmlExtractor

  @transient private val modsXml = new ThreadLocal[Option[Elem]]() {
    override def initialValue(): Option[Elem] = None
  }

  override def preMap(data: Document[JValue]): Document[JValue] = {
      val root = modsRoot(data)
      val xmlString = json
        .extractString(root)
        .getOrElse(throw new Exception(s"No MODS XML for $data"))
        .trim
      val xml = XML.loadString(xmlString)

      modsXml.set(Some(xml))
      data
    }

  override def postMap(data: Document[JValue]): Unit = modsXml.set(None)

  private def getMods =
    modsXml.get().getOrElse(throw new Exception("No MODS XML found"))

  private def modsRoot(data: JValue): JValue =
    data \ "solr_doc_hash" \ "mods_st"

  private def solrRoot(data: JValue): JValue = data \ "solr_doc_hash"

  /** Does the provider use a prefix (typically their provider
    * shortname/abbreviation) to salt the base identifier?
    *
    * @return
    *   Boolean
    */
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("nypl")

  /** Extract the record's "persistent" identifier. Implementations should raise
    * an Exception if no ID can be extracted
    *
    * @return
    *   String Record identifier
    * @throws Exception
    *   If ID can not be extracted
    */
  override def originalId(implicit
      data: utils.Document[JValue]
  ): ZeroToOne[String] = json.extractString("uuid")(data)

  val formatBlockList: Set[String] =
    DigitalSurrogateBlockList.termList ++
      FormatTypeValuesBlockList.termList

  val latLongRegex =
    "^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?),\\s*[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$"

  private val creatorRoles = Seq(
    "architect",
    "art copyist",
    "artist",
    "attributed name",
    "author",
    "binder",
    "binding designer",
    "book jacket designer",
    "bookplate designer",
    "calligrapher",
    "cartographer",
    "choreographer",
    "composer",
    "correspondent",
    "costume designer",
    "cover designer",
    "creator",
    "dedicator",
    "designer",
    "director",
    "dissertant",
    "dubious author",
    "engraver",
    "etcher",
    "facsimilist",
    "film director",
    "forger",
    "illustrator",
    "interviewee",
    "interviewer",
    "inventor",
    "landscape architect",
    "librettist",
    "lighting designer",
    "lithographer",
    "lyricist",
    "papermaker",
    "performer",
    "photographer",
    "sculptor",
    "set designer",
    "singer",
    "sound designer",
    "wood engraver",
    "woodcutter"
  )

  private val contributorRoles = Seq(
    "actor",
    "adapter",
    "addressee",
    "analyst",
    "animator",
    "annotator",
    "applicant",
    "arranger",
    "art director",
    "artistic director",
    "assignee",
    "associated name",
    "auctioneer",
    "author in quotations or text extracts",
    "author of afterword, colophon, etc.",
    "author of dialog",
    "author of introduction, etc.",
    "bibliographic antecedent",
    "blurbwriter",
    "book designer",
    "book producer",
    "bookseller",
    "censor",
    "cinematographer",
    "client",
    "collector",
    "collotyper",
    "colorist",
    "commentator",
    "commentator for written text",
    "compiler",
    "complainant",
    "complainant-appellant",
    "complainant-appellee",
    "compositor",
    "conceptor",
    "conductor",
    "conservator",
    "consultant",
    "consultant to a project",
    "contestant",
    "contestant-appellant",
    "contestant-appellee",
    "contestee",
    "contestee-appellant",
    "contestee-appellee",
    "contractor",
    "contributor",
    "copyright claimant",
    "copyright holder",
    "corrector",
    "curator",
    "curator of an exhibition",
    "dancer",
    "data contributor",
    "data manager",
    "dedicatee",
    "defendant",
    "defendant-appellant",
    "defendant-appellee",
    "degree granting institution",
    "degree grantor",
    "delineator",
    "depositor",
    "distributor",
    "donor",
    "draftsman",
    "editor",
    "editor of compilation",
    "editor of moving image work",
    "electrician",
    "electrotyper",
    "engineer",
    "expert",
    "field director",
    "film distributor",
    "film editor",
    "film producer",
    "first party",
    "former owner",
    "funder",
    "geographic information specialist",
    "honoree",
    "host",
    "illuminator",
    "inscriber",
    "instrumentalist",
    "issuing body",
    "judge",
    "laboratory",
    "laboratory director",
    "lead",
    "lender",
    "libelant",
    "libelant-appellant",
    "libelant-appellee",
    "libelee",
    "libelee-appellant",
    "libelee-appellee",
    "licensee",
    "licensor",
    "manufacturer",
    "marbler",
    "markup editor",
    "metadata contact",
    "metal-engraver",
    "moderator",
    "monitor",
    "music copyist",
    "musical director",
    "musician",
    "narrator",
    "opponent",
    "organizer",
    "originator",
    "other",
    "owner",
    "panelist",
    "patent applicant",
    "patent holder",
    "patron",
    "permitting agency",
    "plaintiff",
    "plaintiff-appellant",
    "plaintiff-appellee",
    "platemaker",
    "presenter",
    "printer",
    "printer of plates",
    "printmaker",
    "process contact",
    "producer",
    "production company",
    "production manager",
    "production personnel",
    "programmer",
    "project director",
    "proofreader",
    "publisher",
    "publishing director",
    "puppeteer",
    "radio producer",
    "recording engineer",
    "redaktor",
    "fenderer",
    "feporter",
    "research team head",
    "research team member",
    "researcher",
    "respondent",
    "respondent-appellant",
    "respondent-appellee",
    "responsible party",
    "restager",
    "reviewer",
    "rubricator",
    "scenarist",
    "scientific advisor",
    "screenwriter",
    "scribe",
    "second party",
    "secretary",
    "signer",
    "speaker",
    "sponsor",
    "stage director",
    "stage manager",
    "standards body",
    "stereotyper",
    "storyteller",
    "supporting host",
    "surveyor",
    "teacher",
    "technical director",
    "television director",
    "television producer",
    "thesis advisor",
    "transcriber",
    "translator",
    "type designer",
    "typographer",
    "videographer",
    "voice actor",
    "witness",
    "writer of accompanying material"
  )

  override def title(data: Document[json4s.JValue]): AtLeastOne[String] =
    // titleInfo \ "title" @usage='primary'
    (getMods \ "titleInfo")
      .flatMap(node => xml.getByAttribute(node, "usage", "primary"))
      .flatMap(node => xml.extractStrings(node \ "title"))

  override def alternateTitle(
      data: Document[json4s.JValue]
  ): ZeroToMany[String] =
    // all other title values
    xml
      .extractStrings(getMods \ "titleInfo" \ "title")
      .diff(title(data))

  override def identifier(data: Document[json4s.JValue]): ZeroToMany[String] = {
    val types = Seq(
      "local_imageid",
      "isbn",
      "isrc",
      "isan",
      "ismn",
      "iswc",
      "issn",
      "uri",
      "urn"
    )
    (getMods \ "identifier")
      .filter(node =>
        xml.filterAttributeListOptions(node, "type", types) || xml
          .filterAttributeListOptions(node, "displayLabel", types)
      )
      .flatMap(xml.extractStrings)
  }

  override def description(
      data: Document[json4s.JValue]
  ): ZeroToMany[String] = {
    // note @type='content'
    // abstract
    val xmlData = getMods
    (xmlData \ "note")
      .flatMap(node => xml.getByAttribute(node, "type", "content"))
      .flatMap(xml.extractStrings) ++ xml.extractStrings(xmlData \ "abstract")
  }

  override def isShownAt(
      data: Document[json4s.JValue]
  ): ZeroToMany[EdmWebResource] =
    // For the base URL was https://digitalcollections.nypl.org/items/
    // plus
    // uuid 4d0e0bc0-c540-012f-1857-58d385a7bc34
    // twas ever thus
    // https://digitalcollections.nypl.org/items/4d0e0bc0-c540-012f-1857-58d385a7bc34
    (getMods \ "identifier")
      .flatMap(node => xml.getByAttribute(node, "type", "uuid"))
      .flatMap(xml.extractStrings)
      .map(uuid => s"https://digitalcollections.nypl.org/items/$uuid")
      .map(stringOnlyWebResource)

  override def subject(
      data: Document[json4s.JValue]
  ): ZeroToMany[SkosConcept] = {
    val subjectKeys =
      Seq("topic", "geographic", "temporal", "occupation", "Ohio", "Cincinnati")

    val mods = getMods

    val subjectTitles =
      (mods \ "subject" \ "titleInfo" \ "title").map(node =>
        SkosConcept(
          providedLabel = xml.extractString(node),
          exactMatch = xml.getAttributeValue(node, "valueURI").map(URI).toSeq
        )
      )

    val subjectNames = (mods \ "subject" \ "name" \ "namePart").map(node =>
      SkosConcept(
        providedLabel = xml.extractString(node),
        exactMatch = xml.getAttributeValue(node, "valueURI").map(URI).toSeq
      )
    )

    val subjects = subjectKeys.flatMap(key =>
      (mods \ "subject" \ key).map(node =>
        SkosConcept(
          providedLabel = xml.extractString(node),
          exactMatch = xml.getAttributeValue(node, "valueURI").map(URI).toSeq
        )
      )
    )

    subjects ++ subjectNames ++ subjectTitles
  }

  override def `type`(data: Document[json4s.JValue]): ZeroToMany[String] =
    xml.extractStrings(getMods \ "typeOfResource")

  override def format(data: Document[json4s.JValue]): ZeroToMany[String] =
    xml.extractStrings(getMods \ "physicalDescription" \ "format") ++
      xml.extractStrings(getMods \ "genre")

  override def extent(data: Document[json4s.JValue]): ZeroToMany[String] =
    xml.extractStrings(getMods \ "physicalDescription" \ "extent")

  override def temporal(
      data: Document[json4s.JValue]
  ): ZeroToMany[EdmTimeSpan] =
    xml.extractStrings(getMods \ "subject" \ "temporal").map(stringOnlyTimeSpan)

  override def creator(data: Document[json4s.JValue]): ZeroToMany[EdmAgent] =
    agentHelper(getMods, creatorRoles)

  override def contributor(
      data: Document[json4s.JValue]
  ): ZeroToMany[EdmAgent] =
    agentHelper(getMods, contributorRoles)

  override def collection(
      data: Document[json4s.JValue]
  ): ZeroToMany[DcmiTypeCollection] =
    json
      .extractStrings("rootCollection_s")(solrRoot(data))
      .map(nameOnlyCollection)

  override def date(data: Document[json4s.JValue]): ZeroToMany[EdmTimeSpan] =
    json
      .extractStrings("keyDate_st")(solrRoot(data))
      .map(stringOnlyTimeSpan)

  override def publisher(data: Document[json4s.JValue]): ZeroToMany[EdmAgent] =
    Seq(emptyEdmAgent)

  override def edmRights(data: Document[json4s.JValue]): ZeroToMany[URI] =
    json.extractStrings("useStatementURI_rtxt")(solrRoot(data)).map(URI)

  override def rights(data: Document[json4s.JValue]): AtLeastOne[String] =
    json.extractStrings("useStatementText_rtxt")(solrRoot(data))

  override def place(data: Document[json4s.JValue]): ZeroToMany[DplaPlace] = {
    (getMods \ "subject" \ "geographic").map(node => {
      DplaPlace(
        name = xml.extractString(node),
        exactMatch = xml.getAttributeValue(node, "valueURI").map(URI).toSeq
      )
    })
  }

  override def language(
      data: Document[json4s.JValue]
  ): ZeroToMany[SkosConcept] =
    xml
      .extractStrings(getMods \ "language" \ "languageTerm")
      .map(nameOnlyConcept)

  // OreAggregation
  override def dplaUri(data: Document[json4s.JValue]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def originalRecord(
      data: Document[json4s.JValue]
  ): ExactlyOne[String] = Utils.formatJson(data)

  override def provider(data: Document[json4s.JValue]): ExactlyOne[EdmAgent] =
    agent

  override def preview(
      data: Document[json4s.JValue]
  ): ZeroToMany[EdmWebResource] =
    json.extractStrings("thumbnail_url")(data).map(stringOnlyWebResource)

  override def dataProvider(
      data: Document[json4s.JValue]
  ): ZeroToMany[EdmAgent] = {
    (getMods \ "location" \ "physicalLocation")
      .filterNot(node => xml.filterAttribute(node, "authority", "marcorg"))
      .map(node => xml.getByAttribute(node, "type", "division"))
      .flatMap(xml.extractStrings)
      .map(_.stripSuffix(".").trim)
      .map(dataProvider => s"$dataProvider. The New York Public Library")
      .distinct
      .map(nameOnlyAgent)
  }

  override def sidecar(data: Document[json4s.JValue]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(
      data
    ))

  // Helper method
  def agent: EdmAgent = EdmAgent(
    name = Some("The New York Public Library"),
    uri = Some(URI("http://dp.la/api/contributor/nypl"))
  )

  private def agentHelper(
      data: NodeSeq,
      roles: Seq[String]
  ): ZeroToMany[EdmAgent] =
    (data \ "name")
      .filter(node =>
        xml
          .extractStrings(node \ "role" \ "roleTerm")
          .map(_.toLowerCase.stripSuffix(" ."))
          .exists(roles.contains(_))
      )
      .flatMap(node => xml.extractStrings(node \ "namePart"))
      .map(nameOnlyAgent)
}
