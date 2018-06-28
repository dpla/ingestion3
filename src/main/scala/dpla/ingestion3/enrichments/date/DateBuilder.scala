package dpla.ingestion3.enrichments.date

import java.util.Locale

import org.joda.time.format.{DateTimeFormat, DateTimeFormatter, DateTimeFormatterBuilder}
import org.joda.time.{DateTime, LocalDate, format}

import scala.util.{Failure, Success, Try}

case class EdtfPatternMap(regexPattern: String, dateTimePattern: String, label: String)


object DateBuilderPatterns {
  protected val delim = "\\s*[\\/-]*\\s*"

  val dateTimeFormatPatterns = Array(
    DateTimeFormat.forPattern("yyyy").getParser,
    DateTimeFormat.forPattern("yyyy MM dd").getParser,
    DateTimeFormat.forPattern("yyyy MMM dd").getParser,
    DateTimeFormat.forPattern("yyyy MMM").getParser,
    DateTimeFormat.forPattern("MMM yyyy").getParser
  )

  val formatter: DateTimeFormatter = new DateTimeFormatterBuilder().append(null, dateTimeFormatPatterns).toFormatter
}

class DateBuilder {
  /**
    *
    * @param date
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
    *
    * @param date
    * @return
    */
  def normalizeDate(date: String): String = date.replaceAll("[\\/-]", " ")
}