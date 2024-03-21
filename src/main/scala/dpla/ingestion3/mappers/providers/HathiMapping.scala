package dpla.ingestion3.mappers.providers

import java.net.URL

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, JsonExtractor, MarcXmlMapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.{HttpUtils, Utils}
import org.json4s.jackson.JsonMethods._
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.util.{Success, Try}
import scala.xml._

class HathiMapping extends MarcXmlMapping {

  val isShownAtPrefix: String = "http://catalog.hathitrust.org/Record/"

  override val enforceDataProvider = false;

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: Option[String] = Some("hathitrust")

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    // <controlfield> tag = 001
    controlfield(data, Seq("001"))
      .flatMap(extractStrings)
      .headOption

  // SourceResource mapping

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // <datafield> tag = 700, 710, 711, or 720
    marcFields(data, Seq("700", "710", "711", "720"))
      .filterNot(
        filterSubfields(_, Seq("e")) // exclude subfields with @code=e and...
          .flatMap(extractStrings)
          .exists(
            Seq("aut", "cre").contains(_)
          ) // ...where #text = "aut" or "cre"
      )
      .map(extractStrings)
      .map(_.mkString(" "))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // <datafield> tag = 100, 110, or 111
    marcFields(data, Seq("100", "110", "111"))
      .map(extractStrings)
      .map(_.mkString(" "))
      .map(nameOnlyAgent)

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = {
    // <datafield>    tag = 260   <subfield> code = c
    // <controlfield> tag = 008
    val dDate = marcFields(data, Seq("260"), Seq("c"))
      .flatMap(extractStrings)
      .map(_.cleanupLeadingPunctuation)
      .map(_.cleanupEndingPunctuation)
      .map(_.stripSuffix("."))
      .map(_.stripPrefix("."))
      .map(stringOnlyTimeSpan)

    if (dDate.nonEmpty) dDate // use datafield date if present
    else extractMarcControlDate(data) // else use controlfield date
  }

  override def description(data: Document[NodeSeq]): ZeroToMany[String] =
    // <datafield> tag = any number in the 500s except 538
    marcFields(data, descriptionTags)
      .flatMap(extractStrings)

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    // <datafield> tag = 300 <subfield> code = a or c
    // <datafield> tag = 340 <subfield> code = b
    (marcFields(data, Seq("300"), Seq("a", "c")) ++ marcFields(
      data,
      Seq("340"),
      Seq("b")
    ))
      .map(extractStrings)
      .map(_.mkString(" "))

  override def format(data: Document[NodeSeq]): ZeroToMany[String] = {
    // <leader> #text character at index 6
    // map character to String value in leaderFormats
    val lFormats: Seq[String] = leaderAt(data, 6)
      .flatMap(key => Try { leaderFormats(key) }.toOption)
      .toSeq

    // <controlfield> code = 007 #text character at index 0
    // map character to String value in controlFormats
    val cFormats: Seq[String] = controlAt(data, "007", 0)
      .flatMap(key => Try { controlFormats(key) }.toOption)

    // <datafield> tag = 337 or 338            <subfield> code = a
    // <datafield> tag = any from subjectTags  <subfield> code = v
    val dFormats = (marcFields(data, Seq("337", "338"), Seq("a")) ++ marcFields(
      data,
      subjectTags,
      Seq("v")
    ))
      .flatMap(extractStrings)

    (lFormats ++ cFormats ++ dFormats).distinct
  }

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] = {
    // <datafield>    tag = 050    <subfield> code = a or b   (LC call number)
    // <datafield>    tag = 020    <subfield> code = a        (ISBN)
    // <datafield>    tag = 022    <subfield> code = a        (ISSN)
    // <datafield>    tag = 035    <subfield> code = a
    // <controlfield> tag = 001                               (Hathi)

    val lcIds: Seq[String] = marcFields(data, Seq("050"), Seq("a", "b"))
      .map(extractStrings)
      .map(_.mkString(" "))
      .map("LC call number: " + _)

    val isbnIds: Seq[String] = marcFields(data, Seq("020"), Seq("a"))
      .flatMap(extractStrings)
      .map("ISBN: " + _)

    val issnIds: Seq[String] = marcFields(data, Seq("022"), Seq("a"))
      .flatMap(extractStrings)
      .map("ISSN: " + _)

    val genericIds: Seq[String] = marcFields(data, Seq("035"), Seq("a"))
      .flatMap(extractStrings)

    val hathiIds: Seq[String] = controlfield(data, Seq("001"))
      .flatMap(extractStrings)
      .map("Hathi: " + _)

    lcIds ++ isbnIds ++ issnIds ++ genericIds ++ hathiIds
  }

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    // <datafield> tag = 041 <subfield> code = a #text split at every third character
    val dLang: Seq[String] = marcFields(data, Seq("041"), Seq("a"))
      .flatMap(extractStrings)
      .flatMap(_.grouped(3).toList)

    // <controlfield> tag = 008 #text characters 35-37 if #text length > 37
    val controlText: String = controlfield(data, Seq("008"))
      .flatMap(extractStrings)
      .headOption
      .getOrElse("")

    val cLang: Seq[String] =
      if (controlText.length > 37)
        Seq(
          controlText.slice(35, 38)
        ) // slice is inclusive on first param, exclusive on second
      else Seq()

    (dLang ++ cLang).map(nameOnlyConcept)
  }

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    // <datafield> tag = 651                  <subfield> code = a
    // <datafield> tag = any from subjectTags <subfield> code = z
    (marcFields(data, Seq("651"), Seq("a")) ++ marcFields(
      data,
      subjectTags,
      Seq("z")
    ))
      .flatMap(extractStrings)
      .map(_.stripSuffix("."))
      .distinct
      .map(nameOnlyPlace)

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // <datafield tag=260> <subfield> code = a or b
    marcFields(data, Seq("260"), Seq("a", "b"))
      .map(extractStrings)
      .map(_.mkString(" "))
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] =
    // <datafield> tag = 760 or 787
    marcFields(data, (760 to 787).map(_.toString))
      .map(extractStrings)
      .map(strings => strings.map(_.stripSuffix(".")))
      .map(_.mkString(". "))
      .map(eitherStringOrUri)

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] =
    // <datafield> tag = 506 or 540
    // <datafield> tag = 974          <subfield> code = r
    (marcFields(data, Seq("974"), Seq("r")) ++ marcFields(
      data,
      Seq("506", "540")
    ))
      .flatMap(extractStrings)
      .slice(0, 1)
      .flatMap(key => Try { rightsMapping(key) }.toOption)
      .map(_ + ". Learn more at http://www.hathitrust.org/access_use")

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    // <datafield> tag = any from subjectTags <subfield> where code is a letter (not a number)

    datafield(data, subjectTags)
      .map(extractMarcSubject)
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    // <datafield> tag = 648
    // <datafield> tag = any from subjectTags <subfield code=y>
    (marcFields(data, Seq("648")) ++ marcFields(data, subjectTags, Seq("y")))
      .flatMap(extractStrings)
      .map(_.stripEndingPeriod)
      .map(stringOnlyTimeSpan)

  override def title(data: Document[NodeSeq]): ZeroToMany[String] = {
    // <datafield> tag = 245 <subfield> where code != c
    val d1: NodeSeq = datafield(data, Seq("245"))
      .map(n => n \ "subfield")
      .flatMap(nseq => nseq.filterNot(n => filterAttribute(n, "code", "c")))

    // <datafield> tag = 242 or 240
    val d2: Seq[NodeSeq] = marcFields(data, Seq("242", "240"))

    (d1 +: d2)
      .map(extractStrings)
      .map(_.map(_.cleanupLeadingPunctuation))
      .map(_.map(_.stripEndingPeriod))
      .map(_.mkString(" "))
  }

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] = {
    // <leader>                     #text characters at index 6 and 7
    // <controlfield> tag = 007_01  #text at index 1
    // <controlfield> tag = 008_21  #text at index 21
    // <datafield>    tag = 970     <subfield> code = a
    // Only map <datafield> if <leader> and <controlfield> have no type value
    val lType = extractMarcLeaderType(data)

    if (lType.isDefined)
      lType.toSeq
    else
      marcFields(data, Seq("970"), Seq("a"))
        .flatMap(extractStrings)
        .flatMap(key => Try { typeMapping(key) }.toOption)
        .flatMap(_._2)
  }

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] =
    mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    // <datafield> tag = 974 <subfield> code = u
    marcFields(data, Seq("974"), Seq("u"))
      .flatMap(extractStrings)
      .flatMap(
        _.splitAtDelimiter("\\.").slice(0, 1)
      ) // split at "." and take first value
      .flatMap(key => Try { dataProviderMapping(key) }.toOption)
      .map(nameOnlyAgent)
  }

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    // <controlfield> tag = 001
    controlfield(data, Seq("001"))
      .flatMap(extractStrings)
      .map(isShownAtPrefix + _)
      .map(stringOnlyWebResource)

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] =
    Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = {
    val hathiThumbnailFetcher =
      new HathiThumbnailFetcher(
        originalId(data),
        oclcId(data),
        isbnId(data),
        googlePrefix(data)
      )

    hathiThumbnailFetcher.thumbnailUrl.toSeq.map(stringOnlyWebResource)
  }

  def oclcId(data: Document[NodeSeq]): Option[String] =
    // <datafield> tag = "035"  <subfield> code = "a"
    marcFields(data, Seq("035"), Seq("a"))
      .flatMap(extractStrings)
      .find(_ contains "(OC")
      .map(_.replaceAll("""\(OCo?LC\)(oc[mn]?)?""", ""))

  def isbnId(data: Document[NodeSeq]): Option[String] =
    // <datafield> tag = "020"  <subfield> code = "a"
    // numerical portion of text, e.g. if #text = "8436305477 (set)" isbnId = "8436305477"
    marcFields(data, Seq("020"), Seq("a"))
      .flatMap(extractStrings)
      .headOption
      .flatMap(_.split(" ").find(_.matches("([0-9]*)")))

  def googlePrefix(data: Document[NodeSeq]): Option[String] =
    // <datafield> tag = "974"  <subfield> code = "u"
    marcFields(data, Seq("974"), Seq("u"))
      .flatMap(extractStrings)
      .map(namespace => {
        // e.g. if namespace = "pst.000061785779", then prefixKey = "pst" and barcode = "000061785779"
        val prefixKey: String = namespace.split("\\.").headOption.getOrElse("")
        lazy val barcode: String =
          namespace.split("\\.").lastOption.getOrElse("")

        Try {
          // try to match prefixKey to googlePrefixMapping value
          googlePrefixMapping(prefixKey)
        } match {
          // if initial google prefix is "UCAL", use barcode to update prefix
          case Success(p) => if (p == "UCAL") getUcalPrefix(barcode) else p
          case _          => "" // no match
        }
      })
      .find(_.nonEmpty) // get first prefix

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(
      data
    ))

  // Helper method
  def agent = EdmAgent(
    name = Some("HathiTrust"),
    uri = Some(URI("http://dp.la/api/contributor/hathi"))
  )

  // <datafield> tags for description
  val descriptionTags: Seq[String] =
    (500 to 599).filterNot(_ == 538).map(_.toString)

  // <datafield> tags for subjects
  private val subjectTags: Seq[String] =
    (Seq(600, 630, 650, 651) ++ (610 to 619) ++ (653 to 658) ++ (690 to 699))
      .map(_.toString)

  // type and genre mappings, derived from <datafield>
  private val typeMapping: Map[String, (Option[String], Option[String])] = Map(
    "AJ" -> (Some("Journal"), Some("Text")),
    "AN" -> (Some("Newspaper"), Some("Text")),
    "BI" -> (Some("Biography"), Some("Text")),
    "BK" -> (Some("Book"), Some("Text")),
    "CF" -> (Some("Computer File"), Some("Interactive Resource")),
    "CR" -> (Some("CDROM"), Some("Interactive Resource")),
    "CS" -> (Some("Software"), Some("Software")),
    "DI" -> (Some("Dictionaries"), Some("Text")),
    "DR" -> (Some("Directories"), Some("Text")),
    "EN" -> (Some("Encyclopedias"), Some("Text")),
    "HT" -> (Some("HathiTrust"), None),
    "MN" -> (Some("Maps-Atlas"), Some("Image")),
    "MP" -> (Some("Map"), Some("Image")),
    "MS" -> (Some("Musical Score"), Some("Text")),
    "MU" -> (Some("Music"), Some("Text")),
    "MV" -> (Some("Archive"), Some("Collection")),
    "MW" -> (Some("Manuscript"), Some("Text")),
    "MX" -> (Some("Mixed Material"), Some("Collection")),
    "PP" -> (Some("Photograph/Pictorial Works"), Some("Image")),
    "RC" -> (Some("Audio CD"), Some("Sound")),
    "RL" -> (Some("Audio LP"), Some("Sound")),
    "RM" -> (Some("Music"), Some("Sound")),
    "RS" -> (Some("Spoken word"), Some("Sound")),
    "RU" -> (None, Some("Sound")),
    "SE" -> (Some("Serial"), Some("Text")),
    "SX" -> (Some("Serial"), Some("Text")),
    "VB" -> (Some("Video (Blu-ray)"), Some("Moving Image")),
    "VD" -> (Some("Video (DVD)"), Some("Moving Image")),
    "VG" -> (Some("Video Games"), Some("Moving Image")),
    "VH" -> (Some("Video (VHS)"), Some("Moving Image")),
    "VL" -> (Some("Motion Picture"), Some("Moving Image")),
    "VM" -> (Some("Visual Material"), Some("Image")),
    "WM" -> (Some("Microform"), Some("Text")),
    "XC" -> (Some("Conference"), Some("Text")),
    "XS" -> (Some("Statistics"), Some("Text"))
  )

  private val dataProviderMapping: Map[String, String] = Map(
    "bc" -> "Boston College",
    "chi" -> "University of Chicago",
    "coo" -> "Cornell University",
    "dul1" -> "Duke University",
    "gri" -> "Getty Research Institute",
    "hvd" -> "Harvard University",
    "ien" -> "Northwestern University",
    "inu" -> "Indiana University",
    "loc" -> "Library of Congress",
    "mdl" -> "Minnesota Digital Library",
    "mdp" -> "University of Michigan",
    "miua" -> "University of Michigan",
    "miun" -> "University of Michigan",
    "nc01" -> "University of North Carolina",
    "ncs1" -> "North Carolina State University",
    "njp" -> "Princeton University",
    "nnc1" -> "Columbia University",
    "nnc2" -> "Columbia University",
    "nyp" -> "New York Public Library",
    "psia" -> "Penn State University",
    "pst" -> "Penn State University",
    "pur1" -> "Purdue University",
    "pur2" -> "Purdue University",
    "uc1" -> "University of California",
    "uc2" -> "University of California",
    "ucm" -> "Universidad Complutense de Madrid",
    "ufl1" -> "University of Florida",
    "uiug" -> "University of Illinois",
    "uiuo" -> "University of Illinois",
    "umn" -> "University of Minnesota",
    "usu" -> "Utah State University Press",
    "uva" -> "University of Virginia",
    "wu" -> "University of Wisconsin",
    "yale" -> "Yale University"
  )

  private val rightsMapping: Map[String, String] = Map(
    "pd" -> "Public domain",
    "ic-world" -> "In-copyright and permitted as world viewable by the copyright holder",
    "pdus" -> "Public domain only when viewed in the US",
    "cc-by" -> "Creative Commons Attribution license",
    "cc-by-nd" -> "Creative Commons Attribution-NoDerivatives license",
    "cc-by-nc-nd" -> "Creative Commons Attribution-NonCommercial-NoDerivatives license",
    "cc-by-nc" -> "Creative Commons Attribution-NonCommercial license",
    "cc-by-nc-sa" -> "Creative Commons Attribution-NonCommercial-ShareAlike license",
    "cc-by-sa" -> "Creative Commons Attribution-ShareAlike license",
    "cc-zero" -> "Creative Commons Zero license (implies pd)",
    "und-world" -> "undetermined copyright status and permitted as world viewable by the depositor"
  )

  private val googlePrefixMapping: Map[String, String] = Map(
    "chi" -> "CHI",
    "coo" -> "CORNELL",
    "hvd" -> "HARVARD",
    "ien" -> "NWU",
    "inu" -> "IND",
    "mdp" -> "UOM",
    "nnc1" -> "COLUMBIA",
    "nyp" -> "NYPL",
    "pst" -> "PSU",
    "pur1" -> "PURD",
    "uc1" -> "UCAL",
    "ucm" -> "UCM",
    "umn" -> "MINN",
    "uva" -> "UVA",
    "wu" -> "WISC"
  )

  private def getUcalPrefix(barcode: String): String = {
    if (barcode.length == 11 && barcode.startsWith("l")) "UCLA"
    else if (barcode.length == 10) "UCB"
    else if (barcode.length == 14) {
      barcode.slice(1, 5) match {
        case "1822" => "UCSD"
        case "1970" => "UCI"
        case "1378" => "UCSF"
        case "2106" => "UCSC"
        case "1205" => "UCSB"
        case "1175" => "UCD"
        case "1158" => "UCLA"
        case "1210" => "UCR"
        case _      => "UCAL"
      }
    } else "UCAL"
  }
}

class HathiThumbnailFetcher(
    hathiIdOpt: Option[String],
    oclcIdOpt: Option[String],
    isbnIdOpt: Option[String],
    googlePrefixOpt: Option[String]
) extends JsonExtractor {

  val baseUrl: String = "http://books.google.com/books?jscmd=viewapi&bibkeys="

  val hathiId: String = hathiIdOpt.getOrElse("")
  val oclcId: String = oclcIdOpt.getOrElse("")
  val isbnId: String = isbnIdOpt.getOrElse("")
  val googlePrefix: String = googlePrefixOpt.getOrElse("")

  // Thumbnail URL request:
  // Base URL: http://books.google.com/books?jscmd=viewapi&bibkeys=<params>
  // Where params are: <google_prefix>:<hathi_id>,OCLC<oclc_id>,ISBN:<isbn>
  // ISBN ID is optional
  val requestUrl: Option[String] = {
    if (hathiId.isEmpty || oclcId.isEmpty || googlePrefix.isEmpty) None
    else {
      val isbnSuffix = if (isbnId.isEmpty) "" else ",ISBN:" + isbnId
      val url =
        baseUrl + googlePrefix + ":" + hathiId + ",OCLC:" + oclcId + isbnSuffix
      Some(url)
    }
  }

  // Control flow for entire process of constructing request URL, sending request, and processing response.
  val thumbnailUrl: Option[String] = requestUrl
    .flatMap(googleResponse(_).toOption)
    .flatMap(parseResponse(_).toOption)
    .flatMap(extractUrl(_))

  // Make GET request to Google Books
  def googleResponse(requestUrl: String): Try[String] = {
    val userAgent: String =
      "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:20.0) Gecko/20100101 Firefox/20.0"
    val headers: Option[Map[String, String]] = Some(
      Map("User-agent" -> userAgent)
    )
    HttpUtils.makeGetRequest(new URL(requestUrl), headers)
  }

  // Parse JSON response from Google Books
  def parseResponse(response: String): Try[JValue] = Try {
    val parsable: String = response
      .replace("var _GBSBookInfo = ", "")
      .replace(";", "")
    parse(parsable)
  }

  // Extract thumbnail URL from JSON
  def extractUrl(json: JValue): Option[String] = {
    val root = "OCLC:" + oclcId
    extractString(json \ root \ "thumbnail_url")
  }
}
