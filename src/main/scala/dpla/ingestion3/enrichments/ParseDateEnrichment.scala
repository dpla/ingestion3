package dpla.ingestion3.enrichments

import java.text.{DateFormat, SimpleDateFormat}
import scala.util.{Failure, Success, Try}
import ParseDateEnrichment._
import dpla.ingestion3.model.EdmTimeSpan
import dpla.ingestion3.utils.EDTFDate

import scala.annotation.tailrec
import scala.util.matching.Regex

class ParseDateEnrichment extends Serializable {

  def parse(
      originalDate: EdmTimeSpan
  ): EdmTimeSpan = {

    val dateString = originalDate.originalSourceDate.getOrElse("")
    val str = preprocess(dateString)

    EDTFDate.rangeForEDTF(str) match {
      case Some(range) =>
        val begin = range.begin match {
          case "" => None
          case _  => Some(range.begin)
        }
        val end = range.end match {
          case "" => None
          case _  => Some(range.end)
        }
        return EdmTimeSpan(
          originalSourceDate = Some(dateString),
          prefLabel = Some(str),
          begin = begin,
          end = end
        )
      case None => ()
    }

    // if we can get an interval out of this, return that
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

    // helper function to return a string
    def timespanify(result: String): Option[EdmTimeSpan] =
      Some(
        EdmTimeSpan(
          prefLabel = Some(result),
          originalSourceDate = originalDate.originalSourceDate,
          // TODO: Get these values out of enrichment
          begin = None,
          end = None
        )
      )

    // calling flatMap on an option doesn't iterate, so each of these chains
    // short circuits the rest of the execution if the result is Some(string)

    parseDate(str).flatMap(timespanify).foreach(return _)
    decadeHyphen(str).flatMap(timespanify).foreach(return _)
    monthYear(str).flatMap(timespanify).foreach(return _)
    decadeString(str).flatMap(timespanify).foreach(return _)
    hyphenatedPartialRange(str).flatMap(timespanify).foreach(return _)
    circa(str).flatMap(timespanify).foreach(return _)

    // by default, return the input if we get here
    originalDate

  }

  private def preprocess(str: String): String = {

    val removedLateAndEarly =
      str
        .replaceAll("[lL]ate", "")
        .replaceAll("[eE]arly", "")
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
    val cleaned =
      str.replaceAll(""".*[cC]{1}[irca\.]*""", "").replaceAll(""".*about""", "")
    parseDate(cleaned) match {
      case Some(_) => Some(cleaned)
      case None       => None
    }
    // todo EDTF stuff
  }

  private def parseInterval(str: String): Option[(String, String)] = {
    rangeMatch(str) match {
      case Some((begin, end)) =>
        (parseDate(begin), parseDate(end)) match {
          case (Some(b), Some(e)) => Some(b, e)
          case _                  => None
        }
      case None => None
    }
  }

  private def rangeMatch(str: String): Option[(String, String)] = {
    val cleanedString = str.replace("to", "-").replace("until", "-")
    rangeMatchRexp.findFirstMatchIn(cleanedString) match {
      case Some(matched) => Some((matched.group(1), matched.group(2)))
      case None          => None
    }
  }

  private def parseDate(str: String): Option[String] = {
    val trialFormats = new ThreadLocal[List[DateFormat]]()
    trialFormats.set(List(
      "yyyy-MM-dd",
      "MMM dd, yyyy",
      "MM/dd/yyyy",
      "MM.dd.yyyy",
      "MM-dd-yyyy",
      "MMM, yyyy",
      "yyyy"
    ).map(s => {
      val df = new SimpleDateFormat(s)
      df.setLenient(false)
      df
    }))

    val responseFormat = new ThreadLocal[DateFormat]()
    responseFormat.set(new SimpleDateFormat("yyyy-MM-dd"))
    responseFormat.get().setLenient(false)

    @tailrec
    def innerParseDate(str: String, formats: List[DateFormat]): Option[String] =
      formats match {
        case head :: rest =>
          Try(head.parse(str)) match {
            case Success(date) =>
              Some(responseFormat.get().format(date))
            case Failure(_) =>
              innerParseDate(str, rest)
          }
        case Nil => None
      }

    innerParseDate(str, trialFormats.get())
  }

  private def monthYear(str: String): Option[String] = {
    monthYearRegex.findFirstMatchIn(str) match {
      case Some(matched) =>
        Some("%s-%s".format(matched.group(2), matched.group(1)))
      case None => None
    }
  }

  private def hyphenatedPartialRange(str: String): Option[String] = {
    hyphenatedPartialRangeRegex.findFirstMatchIn(str) match {
      case Some(matched) =>
        Some(
          "%s-%s/%s-%s".format(
            matched.group(1),
            matched.group(2),
            matched.group(1),
            matched.group(3)
          )
        )
      case None => None
    }
  }

  private def decadeString(str: String): Option[String] = innerDecade(str, decadeStringRegex)
  private def decadeHyphen(str: String): Option[String] = innerDecade(str, decadeHyphenRegex)
  private def innerDecade(string: String, regex: Regex): Option[String] =
    regex.findFirstMatchIn(string) match {
      case Some(matched) => Some(matched.group(1) + "x")
      case None          => None
    }
}


object ParseDateEnrichment {

  private val rangeMatchRexp =
    """([a-zA-Z]{0,3}\s?[\d\-\/\.xu\?\~a-zA-Z]*,?\s?\d{3}[\d\-xs][s\d\-\.xu\?\~]*)\s*[-\.]+\s*([a-zA-Z]{0,3}\s?[\d\-\/\.xu\?\~a-zA-Z]*,?\s?\d{3}[\d\-xs][s\d\-\.xu\?\~]*)""".r

  private val monthYearRegex = """^(\d{2})-(\d{4})$""".r

  private val hyphenatedPartialRangeRegex = """^(\d{2})(\d{2})-(\d{2})$""".r

  private val decadeStringRegex = """^(\d{3})0s$""".r

  private val decadeHyphenRegex = """^(\d{3})-$""".r

}
