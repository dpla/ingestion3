package dpla.ingestion3.mappers.providers

import dpla.ingestion3.enrichments.normalizations.StringNormalizationUtils._
import dpla.ingestion3.mappers.utils.{Document, MarcXmlMapping}
import dpla.ingestion3.model.DplaMapData._
import dpla.ingestion3.model._
import dpla.ingestion3.utils.Utils
import org.json4s.JValue
import org.json4s.JsonDSL._

import scala.util.Try
import scala.xml._


class GpoMapping extends MarcXmlMapping {

  // ID minting functions
  override def useProviderName: Boolean = true

  override def getProviderName: String = "gpo"

  override def originalId(implicit data: Document[NodeSeq]): ZeroToOne[String] =
    extractString(data \ "header" \ "identifier")

  // SourceResource mapping

  override def contributor(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // <datafield> tag = 700, 710, or 711
    marcFields(data, Seq("700", "710", "711"))
      .filter(filterSubfields(_, Seq("e")) // include if subfield with @code=e exists and...
        .flatMap(extractStrings)
        .map(_.stripSuffix("."))
        .exists(_ != "author") // ...#text != "author"
      )
      .map(extractStrings)
      .map(_.mkString(" "))
      .map(nameOnlyAgent)

  override def creator(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = {
    // <datafield> tag = 100, 110, or 111
    // <datafield> tag = 700, 710, or 711
    val creator1xx = marcFields(data, Seq("100", "110", "111"))

    val creator7xx = marcFields(data, Seq("700", "710", "711"))
      .filter(filterSubfields(_, Seq("e")) // include if subfield with @code=e exists and...
        .flatMap(extractStrings)
        .map(_.stripSuffix("."))
        .exists(_ == "author") // ...#text = "author"
      )

    (creator1xx ++ creator7xx)
      .map(extractStrings)
      .map(_.mkString(" "))
      .map(nameOnlyAgent)
  }

  override def date(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] = {
    // <datafield> tag = 362
    // <datafield> tag = 260 or 264   <subfield> code = c
    // <controlfield> tag = 008
    val date362 = marcFields(data, Seq("362"))
    val date260 = marcFields(data, Seq("260"), Seq("c"))
    val date264 = marcFields(data, Seq("264"), Seq("c"))

    val dDateNodes =
      if (date362.nonEmpty) date362
      else if (leaderAt(data, 7).contains('m'))
        if (date260.nonEmpty) date260
        else date264
      else Seq()

    val dDate = dDateNodes
      .flatMap(extractStrings)
      .map(_.stripUnmatchedClosingBrackets)
      .map(_.stripSuffix("."))
      .map(stringOnlyTimeSpan)

    if (dDate.nonEmpty) dDate // use datafield date if present
    else extractMarcControlDate(data) // else use controlfield date
  }

  override def description(data: Document[NodeSeq]): ZeroToMany[String] = {
    // <datafield> tag = 255, 310, 500 to 537, 539 to 582, or 584 to 599
    // <datafield> tag = 583    <subfield> code  = z
    val descTags = (Seq(255) ++ (500 to 537) ++ (539 to 582) ++ (584 to 599)).map(_.toString)

    val desc310 = marcFields(data, Seq("310"))
    val desc583 = marcFields(data, Seq("583"), Seq("z"))
    val desc5xx = marcFields(data, descTags)

    // Use 310 and/or 583 if they exist.  If not, use 5xx.
    val baseDesc =
      (if ((desc310 ++ desc583).nonEmpty) desc310 ++ desc583 else desc5xx)
        .flatMap(extractStrings)

    // Add description frequency if desc310 does not exist, <leader> at index 7 = 's',
    // and a description frequency key is present in <controlfield> 008_18
    val leader7: Option[Char] = leaderAt(data, 7)
    val controlKey: Option[String] = controlfield(data,Seq("008_18"))
      .flatMap(extractStrings)
      .headOption

    val freq: Option[String] = controlKey match {
      case Some(k) => descFrequency.get(k)
      case None => None
    }

    val theDesc =
      if (desc310.isEmpty && leader7.contains('s') && freq.isDefined)
        freq match {
          case Some(f) => baseDesc :+ f
          case None => baseDesc
        }
      else baseDesc

    theDesc.distinct
  }

  private val descFrequency = Map(
    "a" -> "Annual",
    "b" -> "Bimonthly",
    "c" -> "Semiweekly",
    "d" -> "Daily",
    "e" -> "Biweekly",
    "f" -> "Semiannual",
    "g" -> "Biennial",
    "h" -> "Triennial",
    "i" -> "Three times a week",
    "j" -> "Three times a month",
    "k" -> "Continuously updated",
    "m" -> "Monthly",
    "q" -> "Quarterly",
    "s" -> "Semimonthly",
    "t" -> "Three times a year",
    "u" -> "Unknown",
    "w" -> "Weekly",
    "z" -> "Other"
  )

  override def extent(data: Document[NodeSeq]): ZeroToMany[String] =
    // <datafield> tag = 300  <subfield> code = a
    marcFields(data, Seq("300"), Seq("a"))
      .map(extractStrings)
      .map(_.mkString(" "))
      .map(_.stripSuffix(":"))
      .map(_.stripSuffix(";"))

  override def format(data: Document[NodeSeq]): ZeroToMany[String] = {
    // <leader> #text character at index 6
    // map character to String value in leaderFormats
    val lFormats: Seq[String] = leaderAt(data, 6)
      .flatMap(key => Try{ leaderFormats(key) }.toOption)
      .toSeq

    // <controlfield> code = 007 #text character at index 0
    // map character to String value in controlFormats
    val cFormats: Seq[String] = controlAt(data, "007", 0)
      .flatMap(key => Try{ controlFormats(key) }.toOption)

    // <datafield> tag = 337, 338, or 340   <subfield> code = a
    val dFormats = marcFields(data, Seq("337", "338", "340"), Seq("a"))
      .flatMap(extractStrings)

    (lFormats ++ cFormats ++ dFormats).distinct
  }

  override def identifier(data: Document[NodeSeq]): ZeroToMany[String] = {
    // <datafield> tag = 001
    // <datafield> tag = 050                    <subfield> code = a   (LC call number)
    // <datafield> tag = 020                                          (ISBN)
    // <datafield> tag = 022                    <subfield> code = a   (ISSN)
    // <datafield> tag = 035, 074, 082, or 086  <subfield> code = a

    val lcIds = marcFields(data, Seq("050"), Seq("a"))
      .map(extractStrings)
      .map(_.mkString(" "))
      .map("LC call number: " + _)

    val isbnIds = marcFields(data, Seq("020"))
      .map(extractStrings)
      .map(_.mkString(" "))
      .map("ISBN: " + _)

    val issnIds = marcFields(data, Seq("022"))
      .map(extractStrings)
      .map(_.mkString(" "))
      .map("ISSN: " + _)

    val genericIds = (marcFields(data, Seq("001")) ++ marcFields(data, Seq("035", "074", "082", "086"), Seq("a")))
      .map(extractStrings)
      .map(_.mkString(" "))

    lcIds ++ isbnIds ++ issnIds ++ genericIds
  }

  override def language(data: Document[NodeSeq]): ZeroToMany[SkosConcept] = {
    // <datafield> tag = 041 #text split at every third character
    // <datafield> tag = 546

    val lang546 = marcFields(data, Seq("546"))
      .flatMap(extractStrings)
      .map(_.stripPrefix("Text in "))
      .map(_.stripSuffix("."))

    val lang041 = marcFields(data, Seq("041"))
      .flatMap(extractStrings)
      .flatMap(_.grouped(3).toList)

    (lang546 ++ lang041)
      .map(nameOnlyConcept)
  }

  override def place(data: Document[NodeSeq]): ZeroToMany[DplaPlace] =
    // <datafield> tag = 650  <subfield> code = z
    // <datafield> tag = 651  <subfield> code = a
    (marcFields(data, Seq("650"), Seq("z")) ++ marcFields(data, Seq("651"), Seq("a")))
      .flatMap(extractStrings)
      .map(_.stripSuffix("."))
      .map(nameOnlyPlace)
      .distinct

  override def publisher(data: Document[NodeSeq]): ZeroToMany[EdmAgent] =
    // <datafield> tag = 260 or 264   <subfield> code = a or b
    marcFields(data, Seq("260", "264"), Seq("a", "b"))
      .map(extractStrings)
      .map(_.mkString(" "))
      .map(nameOnlyAgent)

  override def relation(data: Document[NodeSeq]): ZeroToMany[LiteralOrUri] = {
    // <datafield> tag = 490, 730, 740, 830, or 760 to 786
    val relationTags = (Seq(490, 730, 740, 830) ++ (760 to 786)).map(_.toString)

    marcFields(data, relationTags)
      .map(extractStrings)
      .map(_.map(_.stripSuffix("."))) // remove trailing "."
      .map(_.mkString(". ") + ".") // join with "." and add "." at end
      .map(eitherStringOrUri)
  }

  override def rights(data: Document[NodeSeq]): AtLeastOne[String] = {
    // <datafield> tag = 506
    val rights506 = marcFields(data, Seq("506"))
      .flatMap(extractStrings)

    val theRights = if (rights506.isEmpty) Seq(default_rights_statement) else rights506

    // Do not map records that have incompatible rights statements.
    theRights.foreach(r =>
      if (excludedRights.contains(r)) throw new Exception("Incompatible rights statement")
    )

    theRights
  }

  private val default_rights_statement: String =
    "Pursuant to Title 17 Section 105 of the United States " +
      "Code, this file is not subject to copyright protection " +
      "and is in the public domain. For more information " +
      "please see http://www.gpo.gov/help/index.html#" +
      "public_domain_copyright_notice.htm"

  private val excludedRights = Array(
    "Access may require a library card.",

    "Access restricted to U.S. military service members and Dept. " +
      "of Defense employees; the only issues available are those " +
      "from Monday of the prior week through Friday of current week.",

    "Access to data provided for a fee except for requests " +
      "generated from Internet domains of .gov, .edu, .k12, .us, " +
      "and .mil.",

    "Access to issues published prior to 2001 restricted to " +
      "Picatinny Arsenal users.",

    "Access to some volumes or items may be restricted",

    "Document removed from the GPO Permanent Access Archive at " +
      "agency request",

    "FAA employees only.",

    "First nine issues (Apr.-Dec. 2002) were law enforcement " +
      "restricted publications and are not available to the general " +
      "public.",

    "Free to users at U.S. Federal depository libraries; other " +
      "users are required to pay a fee.",

    "Full text available to subscribers only.",

    "Login and password required to access web page where " +
      "electronic files may be downloaded.",

    "Login and password required to access web page where " +
      "electronic formats may be downloaded.",

    "Not available for external use as of Monday, Oct. 20, 2003.",

    "Personal registration and/or payment required to access some " +
      "features.",

    "Restricted access for security reasons",

    "Restricted to Federal depository libraries and other users " +
      "with valid user accounts.",

    "Restricted to federal depository libraries with valid user " +
      "IDs and passwords.",

    "Restricted to institutions with a site license to the USA " +
      "trade online database. Free to users at federal depository " +
      "libraries.",

    "Some components of this directory may not be publicly " +
      "accessible.",

    "Some v. are for official use only, i.e. distribution of Oct. " +
      "1998 v. 2 is restricted.",

    "Special issue for Oct./Dec. 2007 for official use only.",

    "Subscription required for access."
  )

  override def subject(data: Document[NodeSeq]): ZeroToMany[SkosConcept] =
    // <datafield> tag = 600, 610, 611, 630, 650, or 651  <subfield> code is a letter (not a number)
    datafield(data, Seq("600", "610", "611", "630", "650", "651"))
      .map(extractMarcSubject)
      .map(nameOnlyConcept)

  override def temporal(data: Document[NodeSeq]): ZeroToMany[EdmTimeSpan] =
    // <datafield> tag = 600, 610, 650, or 651  <subfield> code = y
    // <datafield> tag = 611                    <subfield> code = d
    (marcFields(data, Seq("600", "610", "650", "651"), Seq("y")) ++ marcFields(data, Seq("611"), Seq("d")))
      .flatMap(extractStrings)
      .map(stringOnlyTimeSpan)
      .distinct

  override def title(data: Document[NodeSeq]): ZeroToMany[String] =
    // <datafield> tag = 245  <subfield> code != (c or h)
    marcFields(data, Seq("245"))
      .flatMap(nseq => nseq.filterNot(n => filterAttribute(n, "code", "c")))
      .flatMap(nseq => nseq.filterNot(n => filterAttribute(n, "code", "h")))
      .map(extractStrings)
      .map(_.mkString(" "))

  override def `type`(data: Document[NodeSeq]): ZeroToMany[String] = {
    // <leader>                     #text characters at index 6 and 7
    // <controlfield> tag = 007_01  #text at index 1
    // <controlfield> tag = 008_21  #text at index 21
    // <datafield> tag = 337        <subfield> code = a
    // <datafield> tag = 655
    // Only map <datafield> if <leader> and <controlfield> have no type value
    val lType = extractMarcLeaderType(data)

    if (lType.isDefined)
      lType.toSeq
    else
      (marcFields(data, Seq("337"), Seq("a")) ++ marcFields(data, Seq("655")))
        .flatMap(extractStrings)
  }

  // OreAggregation
  override def dplaUri(data: Document[NodeSeq]): ZeroToOne[URI] = mintDplaItemUri(data)

  override def dataProvider(data: Document[NodeSeq]): ZeroToMany[EdmAgent] = Seq(agent)

  override def isShownAt(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] = {
    val uriPrefix: String = "http://catalog.gpo.gov/F/?func=direct&doc_number="
    val uriSuffix: String = "&format=999"

    val cIsShownAt: Option[String] = controlfield(data, Seq("001"))
      .flatMap(extractStrings)
      .headOption

    cIsShownAt match {
      case Some(c) => Seq(uriPrefix + c + uriSuffix).map(stringOnlyWebResource)
      case None => Seq()
    }
  }

  override def originalRecord(data: Document[NodeSeq]): ExactlyOne[String] = Utils.formatXml(data)

  override def preview(data: Document[NodeSeq]): ZeroToMany[EdmWebResource] =
    // every record gets the same preview thumbnail
    Seq("http://fdlp.gov/images/gpo-tn.jpg")
      .map(stringOnlyWebResource)

  override def provider(data: Document[NodeSeq]): ExactlyOne[EdmAgent] = agent

  override def sidecar(data: Document[NodeSeq]): JValue =
    ("prehashId" -> buildProviderBaseId()(data)) ~ ("dplaId" -> mintDplaId(data))

  // Helper method
  def agent = EdmAgent(
    name = Some("United States Government Publishing Office (GPO)"),
    uri = Some(URI("http://dp.la/api/contributor/gpo"))
  )
}
