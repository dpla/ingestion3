package dpla.ingestion3.enrichments.date

import org.joda.time.LocalDate
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, DateTimeFormatterBuilder}

import scala.util.{Failure, Success, Try}

/**
  *
  * @param regexPattern Patten to match against
  * @param dateTimePattern Pattern to transform into
  * @param label Label for this pattern to pattern transformation
  */
case class EdtfPatternMap(regexPattern: String, dateTimePattern: String, label: String)

/**
  *
  */
object DateBuilderPatterns {
  // Support date patterns
  val dateTimeFormatPatterns = Array(
    // Single date
    DateTimeFormat.forPattern("yyyy").getParser,
    DateTimeFormat.forPattern("yyyy MM").getParser,
    DateTimeFormat.forPattern("yyyy MM dd").getParser,
    DateTimeFormat.forPattern("yyyy MMM").getParser,
    DateTimeFormat.forPattern("yyyy MMM dd").getParser,
    DateTimeFormat.forPattern("MMM yyyy").getParser


    // TODO Patterns to be implemented
    // Date ranges
    //  yyyy-yyyy | 1935-1956
    //  yyyy-MM-dd/yyyy-MM-dd | 1850-01-01/1950-12-31

    // Periods
    //  century -> | 19th century

    // Indeterminate
    //  "n.d." -> "indeterminate (n.d.)", // n.d.
    //  "un" -> "indeterminate (un*)", // unknown
    //  "no" -> "indeterminate (no*)", // no(t) dated
    //  "^[a-zA-Z]*$" -> "indeterminate (no digits)"  // early bronze age
  )

  val formatter: DateTimeFormatter = new DateTimeFormatterBuilder().append(null, dateTimeFormatPatterns).toFormatter
}

/**
  *
  */
class DateBuilder {
  protected val delimiters = "\\s*[\\/-]*\\s*"
  /**
    * Parses a string value into a Joda LocalDate object
    *
    * @param date Original date value to parse
    * @return
    */
  def buildDateObject(date: String): Option[LocalDate] = {

    val normalizedDate = normalizeDate(date)

    Try { DateBuilderPatterns.formatter.parseDateTime(normalizedDate)} match {
      case Success(formattedDate) => Option(formattedDate.toLocalDate)
      case Failure(_) => None
    }
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