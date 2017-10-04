package dpla.ingestion3.enrichments

import java.text.SimpleDateFormat

import scala.util.{Failure, Success, Try}
import ParseDateEnrichment._
import dpla.ingestion3.model.EdmTimeSpan

import scala.annotation.tailrec

class ParseDateEnrichment extends Serializable {

  //TODO ranges
  def parse(edtfOriginalDate: EdmTimeSpan, allowInterval: Boolean = false): EdmTimeSpan = {
    val dateString = edtfOriginalDate.originalSourceDate.getOrElse("")
    val str = preprocess(dateString)

    //if we can get an interval out of this, return that
    parseInterval(str)
      .map(
        { case (begin, end) =>
          return EdmTimeSpan(
            originalSourceDate = Some(dateString),
            prefLabel = Some(dateString),
            begin = Some(begin),
            end = Some(end)
          )
        }
      )


    //helper function to return a string
    def timespanify(result: String): Option[EdmTimeSpan] =
      Some(
        EdmTimeSpan(
          prefLabel = Some(result),
          originalSourceDate = edtfOriginalDate.originalSourceDate,
          // TODO: Get these values out of enrichment
          begin = None,
          end = None
        ))

    //calling flatMap on an option doesn't iterate, so each of these chains
    //short circuits the rest of the execution if the result is Some(string)

    parseDate(str).flatMap(timespanify).foreach(return _)
    //todo full EDTF parsing
    partialEdtf(str).flatMap(timespanify).foreach(return _)
    decadeHyphen(str).flatMap(timespanify).foreach(return _)
    monthYear(str).flatMap(timespanify).foreach(return _)
    decadeString(str).flatMap(timespanify).foreach(return _)
    hyphenatedPartialRange(str).flatMap(timespanify).foreach(return _)
    circa(str).flatMap(timespanify).foreach(return _)

    //by default, return the input if we get here
    edtfOriginalDate

  }


  private def preprocess(str: String): String = {

    val removedLateAndEarly =
      str.replaceAll("[lL]ate", "").replaceAll("[eE]arly", "")
        .trim
        .replaceAll("\\s+", " ")

    val removedDecades =
      if (removedLateAndEarly.matches("""^\d{2,3}0s$"""))
        removedLateAndEarly.replaceAll("""0s\b""", "x")
      else removedLateAndEarly

    // What is the goal of this replacement?  Why turn "1978--" into "1978xx"?
    val removedRanges =
      if (removedDecades.matches("""^\d{2,4}\-+$"""))
        removedDecades.replaceAll("-", "x")
      else removedDecades

    removedRanges
  }

  private def circa(str: String): Option[String] = {
    val cleaned = str.replaceAll(""".*[cC]{1}[irca\.]*""", "").replaceAll(""".*about""", "")
    parseDate(cleaned) match { //todo i removed recusrion by changing parse() to parseDate().
      case Some(date) => Some(cleaned)
      case None => None
    }
    //todo EDTF stuff
  }

  private def parseInterval(str: String): Option[(String, String)] = {
    //todo parse the dates from the range?
    rangeMatch(str) match {
      case Some((begin, end)) =>
        (parseDate(begin), parseDate(end)) match {
          case (Some(b), Some(e)) => Some(b, e)
          case _ => None
        }
      case None => None
    }
  }

  private def rangeMatch(str: String): Option[(String, String)] = {
    val cleanedString = str.replace("to", "-").replace("until", "-")
    rangeMatchRexp.findFirstMatchIn(cleanedString) match {
      case Some(matched) => Some((matched.group(1), matched.group(2)))
      case None => None
    }
  }

  private def parseDate(str: String): Option[String] = {

    //TODO ideally these are ThreadLocal and stick around rather than being rebuilt all the time
    val trialFormats = List(
      "yyyy-MM-dd",
      "MMM dd, yyyy",
      "MM/dd/yyyy",
      "MM.dd.yyyy",
      "MM-dd-yyyy",
      "MMM, yyyy"
    )

    @tailrec
    def innerParseDate(str: String, formats: List[String]): Option[String] =
      formats match {
        case head :: rest =>
          val df = new SimpleDateFormat(head)
          df.setLenient(false)
          val responseFormat = new SimpleDateFormat("yyyy-MM-dd")
          df.setLenient(false)
          Try(df.parse(str)) match {
            case Success(date) =>
              Some(responseFormat.format(date))
            case Failure(exception) =>
              innerParseDate(str, rest)
          }
        case Nil => None
      }

    innerParseDate(str, trialFormats)
  }

  private def monthYear(str: String): Option[String] = {
    monthYearRexp.findFirstMatchIn(str) match {
      case Some(matched) =>
        Some("%s-%s".format(matched.group(2), matched.group(1)))
      case None => None
    }
  }

  private def hyphenatedPartialRange(str: String): Option[String] = {
    hyphenatedPartialRangeRegexp.findFirstMatchIn(str) match {
      case Some(matched) =>
        Some("%s-%s/%s-%s".format(matched.group(1), matched.group(2), matched.group(1), matched.group(3)))
      case None => None
    }
  }

  private def partialEdtf(str: String): Option[String] = {
    partialEdtfRegexp.findFirstMatchIn(str) match {
      case Some(matched) =>
        Some("%s-%s/%s-%s".format(matched.group(1), matched.group(3), matched.group(1), matched.group(4)))
      case None => None
    }
  }

  private def decadeString(str: String): Option[String] = {
    decadeStringRegexp.findFirstMatchIn(str) match {
      case Some(matched) => Some(matched.group(1) + "x")
      case None => None
    }
  }

  private def decadeHyphen(str: String): Option[String] = {
    decadeHyphenRegexp.findFirstMatchIn(str) match {
      case Some(matched) => Some(matched.group(1) + "x")
      case None => None
    }
  }
}

object ParseDateEnrichment {

  val rangeMatchRexp = """([a-zA-Z]{0,3}\s?[\d\-\/\.xu\?\~a-zA-Z]*,?\s?\d{3}[\d\-xs][s\d\-\.xu\?\~]*)\s*[-\.]+\s*([a-zA-Z]{0,3}\s?[\d\-\/\.xu\?\~a-zA-Z]*,?\s?\d{3}[\d\-xs][s\d\-\.xu\?\~]*)""".r

  val monthYearRexp = """^(\d{2})-(\d{4})$""".r

  val hyphenatedPartialRangeRegexp = """^(\d{2})(\d{2})-(\d{2})$""".r

  val partialEdtfRegexp = """^(\d{4}(-\d{2})*)-(\d{2})\/(\d{2})$""".r

  val decadeStringRegexp = """^(\d{3})0s$""".r

  val decadeHyphenRegexp = """^(\d{3})-$""".r
}
