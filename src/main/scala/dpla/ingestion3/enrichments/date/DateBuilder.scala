package dpla.ingestion3.enrichments.date

import java.text.SimpleDateFormat
import dpla.ingestion3.model._
import scala.util.{Failure, Success, Try}

/**
  *
  * @param parser
  * @param format
  */
case class EdtfPatternMap(parser: SimpleDateFormat, format: SimpleDateFormat)

/**
  *
  */
object DateBuilderPatterns {

  val edtfPatterns = Seq(
    EdtfPatternMap(new SimpleDateFormat("yyyy MM dd"),  new SimpleDateFormat("yyyy-MM-dd")),
    EdtfPatternMap(new SimpleDateFormat("yyyy MMM dd"),    new SimpleDateFormat("yyyy-MM-dd")),
    EdtfPatternMap(new SimpleDateFormat("MMM yyyy"),    new SimpleDateFormat("yyyy-MM")),
    EdtfPatternMap(new SimpleDateFormat("yyyy MMM"),    new SimpleDateFormat("yyyy-MM")),
    EdtfPatternMap(new SimpleDateFormat("yyyy MM"),     new SimpleDateFormat("yyyy-MM")),
    EdtfPatternMap(new SimpleDateFormat("yyyy"),        new SimpleDateFormat("yyyy"))

    // TODO Date patterns to be implemented

    // Date ranges
    // yyyy-yyyy | 1935-1956
    // yyyy-MM-dd/yyyy-MM-dd | 1850-01-01/1950-12-31

    // Periods
    //  century -> | 19th century

    // Indeterminate
    //  "n.d." -> "indeterminate (n.d.)", // n.d.
    //  "un" -> "indeterminate (un*)", // unknown
    //  "no" -> "indeterminate (no*)", // no(t) dated
    //  "^[a-zA-Z]*$" -> "indeterminate (no digits)"  // early bronze age
  )
}

/**
  *
  */
class DateBuilder {
  protected val delimiters = "\\s*[\\/-]*\\s*"

  /**
    *
    * @param date
    * @return
    */
  def buildEdmTimeSpan(date: String): EdmTimeSpan = {
    val normalizedDate = normalizeDate(date)
    DateBuilderPatterns.edtfPatterns.foreach(edtf => {
      Try { edtf.parser.parse(normalizedDate) } match {
        case Success(parsedDate) =>
          val formattedDte = edtf.format.format(parsedDate)
          return EdmTimeSpan(
            originalSourceDate = Option(date),
            prefLabel = Option(formattedDte),
            begin = Option(formattedDte),
            end = Option(formattedDte)
          )
        case Failure(f) => None
      }

    })
    stringOnlyTimeSpan(date)
  }

  /**
    * Normalizes a provided date string to facilitate pattern matching
    * - Replace all supported delimiters (forward slash, dash) with whitespace
    *
    * @param date Date string
    * @return Normalized date string
    */
  def normalizeDate(date: String): String = date.replaceAll("[\\/-]", " ")
}