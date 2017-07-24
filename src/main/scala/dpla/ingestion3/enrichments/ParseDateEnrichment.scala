package dpla.ingestion3.enrichments

import java.text.SimpleDateFormat

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}
import ParseDateEnrichment._
import dpla.ingestion3.model.EdmTimeSpan

class ParseDateEnrichment {

  //TODO ranges
  def parse(edtfOriginalDate: EdmTimeSpan, allowInterval: Boolean = false): EdmTimeSpan = {
    val dateString = edtfOriginalDate.originalSourceDate.getOrElse("")
    val str = preprocess(dateString)

    //TODO these lazy vals get dereferenced when put in the Seq below.
    val interval = () => if (allowInterval) parseInterval(str) else None
    val parseDateVal = () => parseDate(str)
    //    date ||= Date.edtf(str.gsub('.', '-')) //todo
    val partialEdtfVal = () => partialEdtf(str)
    val decadeHyphenVal = () => decadeHyphen(str)
    val monthYearVal = () => monthYear(str)
    val decadeStrVal = () => decadeString(str)
    val hyphenatedPartialRangeVal = () => hyphenatedPartialRange(str)
    val circaVal = () => circa(str)

    val options = List(
      parseDateVal,
      partialEdtfVal,
      decadeHyphenVal,
      monthYearVal,
      decadeStrVal,
      hyphenatedPartialRangeVal,
      circaVal
    )

    //this iterates over the list, executes the next function, and returns it if there's a result.
    //otherwise, it continues until the end and returns None.
    //I wasn't able to find a better way to emulate ||= from Ruby. There's probably a better way.
    @tailrec def findFirst(options: List[() => Option[String]]): EdmTimeSpan = options match {
      case x::xs =>
        val result = x()
        if (result.isDefined) {
          EdmTimeSpan(
              prefLabel = result,
              originalSourceDate = edtfOriginalDate.originalSourceDate,
              // TODO: Get these values out of enrichment
              begin = None,
              end = None
            )
        } else findFirst(xs)
      // If no enrichment worked then return the original data
      case Nil => edtfOriginalDate
    }

    findFirst(options)
  }

  private def preprocess(str: String): String = {

    val removedLateAndEarly =
      str.replaceAll("[lL]ate", "").replaceAll("[eE]arly", "")
        .trim
        .replaceAll("\\s+", " ")

    val removedDecades =
      if (removedLateAndEarly.matches("""^[1-9]+0s$"""))
        removedLateAndEarly.replaceAll("0s", "x")
      else removedLateAndEarly

    val removedRanges =
      if (removedDecades.matches("""^[1-9]+\-+$"""))
        removedDecades.replaceAll("-", "x")
      else removedDecades

    removedRanges
  }

  private def rangeMatch(str: String): Option[(String, String)] = {
    val cleanedString = str.replace("to", "-").replace("until", "-")
    rangeMatchRexp.findFirstMatchIn(cleanedString) match {
      case Some(matched) => Some((matched.group(1), matched.group(2)))
      case None => None
    }
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

  private def parseDate(str: String): Option[String] = {
    @tailrec
    def innerParseDate(str: String, formats: List[SimpleDateFormat]): Option[String] =
      formats match {
        case head :: rest =>
          Try(head.parse(str)) match {
            case Success(date) => Some(responseFormat.format(date))
            case Failure(exception) => innerParseDate(str, rest)
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
  val responseFormat = new SimpleDateFormat("yyyy-MM-dd")

  val trialFormats = List(
    new SimpleDateFormat("yyyy-MM-dd"),
    new SimpleDateFormat("MMM dd, yyyy"),
    new SimpleDateFormat("MM/dd/yyyy"),
    new SimpleDateFormat("MM.dd.yyyy"),
    new SimpleDateFormat("MM-dd-yyyy"),
    new SimpleDateFormat("MMM, yyyy")
  )
  trialFormats.foreach(_.setLenient(false))

  val rangeMatchRexp = """([a-zA-Z]{0,3}\s?[\d\-\/\.xu\?\~a-zA-Z]*,?\s?\d{3}[\d\-xs][s\d\-\.xu\?\~]*)\s*[-\.]+\s*([a-zA-Z]{0,3}\s?[\d\-\/\.xu\?\~a-zA-Z]*,?\s?\d{3}[\d\-xs][s\d\-\.xu\?\~]*)""".r

  val monthYearRexp = """^(\d{2})-(\d{4})$""".r

  val hyphenatedPartialRangeRegexp = """^(\d{2})(\d{2})-(\d{2})$""".r

  val partialEdtfRegexp = """^(\d{4}(-\d{2})*)-(\d{2})\/(\d{2})$""".r

  val decadeStringRegexp = """^(\d{3})0s$""".r

  val decadeHyphenRegexp = """^(\d{3})-$""".r
}
