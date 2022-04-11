package dpla.eleanor.mappers

import java.io.ByteArrayInputStream

import dpla.eleanor.Schemata.{HarvestData, MappedData}
import dpla.eleanor.mappers.StandardEbooksMapping.mintDplaId
import dpla.eleanor.mappers.utils.{MarcXmlMapping, XmlExtractor}

import scala.util.{Failure, Success, Try}
import scala.xml.{Elem, XML}

object GpoMapping extends MarcXmlMapping with XmlExtractor {

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

  lazy val providerName = "United States Government Publishing Office (GPO)"

  def id(xml: Elem): String = Option((xml \ "id").text.trim).getOrElse(throw new RuntimeException("Missing required ID"))

  def author(xml: Elem): Seq[String] = {
    // <datafield> tag = 100, 110, or 111
    // <datafield> tag = 700, 710, or 711
    val creator1xx = marcFields(xml, Seq("100", "110", "111"))

    val creator7xx = marcFields(xml, Seq("700", "710", "711"))
      .filter(filterSubfields(_, Seq("e")) // include if subfield with @code=e exists and...
        .flatMap(extractStrings)
        .map(_.stripSuffix("."))
        .exists(_ == "author") // ...#text = "author"
      )

    (creator1xx ++ creator7xx)
      .map(extractStrings)
      .map(_.mkString(" "))
  }

  def language(xml: Elem): Seq[String] = {
    // <datafield> tag = 041 #text split at every third character
    // <datafield> tag = 546

    val lang546 = marcFields(xml, Seq("546"))
      .flatMap(extractStrings)
      .map(_.stripPrefix("Text in "))
      .map(_.stripSuffix("."))

    val lang041 = marcFields(xml, Seq("041"))
      .flatMap(extractStrings)
      .flatMap(_.grouped(3).toList)

    lang546 ++ lang041
  }

  def summary(xml: Elem): Seq[String] = {
    // <datafield> tag = 255, 310, 500 to 537, 539 to 582, or 584 to 599
    // <datafield> tag = 583    <subfield> code  = z
    val descTags = (Seq(255) ++ (500 to 537) ++ (539 to 582) ++ (584 to 599)).map(_.toString)

    val desc310 = marcFields(xml, Seq("310"))
    val desc583 = marcFields(xml, Seq("583"), Seq("z"))
    val desc5xx = marcFields(xml, descTags)

    val baseDesc = (desc310 ++ desc583 ++ desc5xx)
      .flatMap(extractStrings)

    // Add description frequency if desc310 does not exist, <leader> at index 7 = 's',
    // and a description frequency key is present in <controlfield> 008_18
    val leader7: Option[Char] = leaderAt(xml, 7)
    val controlKey: Option[String] = controlfield(xml, Seq("008_18"))
      .flatMap(extractStrings)
      .headOption

    val freq: Option[String] = controlKey match {
      case Some(k) => descFrequency.get(k)
      case None => None
    }

    val theDesc =
      if ((desc310 ++ desc583).isEmpty && leader7.contains('s') && freq.isDefined)
        freq match {
          case Some(f) => baseDesc :+ f
          case None => baseDesc
        }
      else baseDesc

    theDesc.map(_.trim).distinct
  }

  def title(xml: Elem): Seq[String] = {
    // <datafield> tag = 245  <subfield> code != (c or h)
    val joinedTitle = marcFields(xml, Seq("245"))
      .flatMap(nseq => nseq.filterNot(n => filterAttribute(n, "code", "c")))
      .flatMap(nseq => nseq.filterNot(n => filterAttribute(n, "code", "h")))
      .flatMap(extractStrings)
      .mkString(" ")
      .stripSuffix(".")

    Seq(joinedTitle)
  }

  def date(xml: Elem): Seq[String] = {
    // <datafield> tag = 362
    // <datafield> tag = 260 or 264   <subfield> code = c
    // <controlfield> tag = 008
    val date362 = marcFields(xml, Seq("362"))
    val date260 = marcFields(xml, Seq("260"), Seq("c"))
    val date264 = marcFields(xml, Seq("264"), Seq("c"))

    val dDateNodes =
      if (date362.nonEmpty) date362
      else if (leaderAt(xml, 7).contains('m'))
        if (date260.nonEmpty) date260
        else date264
      else Seq()

    val dDate = dDateNodes
      .flatMap(extractStrings)
      .map(_.stripSuffix("."))

    if (dDate.nonEmpty) dDate // use datafield date if present
    else extractMarcControlDate(xml) // else use controlfield date

  }
  def publisher(xml: Elem): Seq[String] = {
    // <datafield> tag = 260 or 264   <subfield> code = a or b
    marcFields(xml, Seq("260", "264"), Seq("a", "b"))
      .map(extractStrings)
      .map(_.mkString(" "))
  }

  def uri(xml: Elem): Seq[String] = {
    val uriPrefix: String = "http://catalog.gpo.gov/F/?func=direct&doc_number="
    val uriSuffix: String = "&format=999"

    val cIsShownAt: Option[String] = controlfield(xml, Seq("001"))
      .flatMap(extractStrings)
      .headOption

    cIsShownAt match {
      case Some(c) => Seq(uriPrefix + c + uriSuffix)
      case None => Seq()
    }
  }

  def map(gpo: HarvestData): Option[MappedData] = {
    val xml = Try {
      XML.load(new ByteArrayInputStream(gpo.metadata))
    } match {
      case Success(x) => x
      case Failure(_) =>
        println(s"Unable to load record XML metadata for ${gpo.id}")
        return None
    }

    // If missing URI log error and return None
    val uri = GpoMapping.uri(xml)
      .headOption
    match {
      case Some(t) => t
      case None =>
        println(s"Missing required sourceUri for ${gpo.id}")
        return None
    }

    Some(
      MappedData(
        id = mintDplaId(id(xml), providerName),
        providerName = providerName,
        sourceUri = gpo.sourceUri,
        timestamp = gpo.timestamp,
        itemUri = uri,
        originalRecord = gpo.metadata,
        payloads = gpo.payloads,

        // record metadata
        title = title(xml),
        author = author(xml),
        language = language(xml),
        summary = summary(xml),
        // genre = genre(xml)
        publicationDate = date(xml)
      )
    )
  }
}
