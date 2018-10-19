package dpla.ingestion3.enrichments.date

import dpla.ingestion3.model._
import scala.util.{Failure, Success, Try}
import java.text.SimpleDateFormat
import java.util.TimeZone

/**
  * Maps expected source date patterns to standard format that is EDTF compliant
  * @param parser Pattern to parse
  * @param format Pattern to format original date to
  */
case class EdtfPatternMap(parser: String, format: String)


/**
  *
  */
object DateBuilderPatterns {

  val edtfPatterns = Seq(
    EdtfPatternMap("yyyy MM dd", "yyyy-MM-dd"),
    EdtfPatternMap("yyyy MMM dd", "yyyy-MM-dd"),
    EdtfPatternMap("MMM yyyy", "yyyy-MM"),
    EdtfPatternMap("yyyy MMM", "yyyy-MM"),
    EdtfPatternMap("yyyy MM", "yyyy-MM"),
    EdtfPatternMap("yyyy", "yyyy")

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
    * @param date String  Date to attempt parsing and formatting as valid ETDF format
    * @return
    */
  def buildEdmTimeSpan(date: String): EdmTimeSpan = {
    val normalizedDate = normalizeDate(date)
    DateBuilderPatterns.edtfPatterns.foreach(edtf => {
      // Perform a simple length comparision
      if(edtf.parser.length == normalizedDate.length) {
        // Get the parser
        val parser = DateFormatCache.get(edtf.parser)

        Try { parser.parse(normalizedDate) } match {
          case Success(parsedDate) =>
            val formatter = DateFormatCache.get(edtf.format)
            val formattedDate = formatter.format(parsedDate)

            return EdmTimeSpan(
              originalSourceDate = Option(date),
              prefLabel = Option(formattedDate),
              begin = Option(formattedDate), // FIXME this will not pass muster for date ranges
              end = Option(formattedDate) // FIXME this will not pass muster for date ranges
            )
          case Failure(_) => None
        }
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


object DateFormatCache {

  private val cache = new ThreadLocal[collection.mutable.Map[String, SimpleDateFormat]]
  cache.set(collection.mutable.Map[String, SimpleDateFormat]())

  def get(formatString: String): SimpleDateFormat = {
    val localCache = cache.get()
    localCache.get(formatString) match {
      case None =>
        val simpleDateFormat = new SimpleDateFormat(formatString)
        simpleDateFormat.setTimeZone(TimeZone.getTimeZone("EST"))
        simpleDateFormat.setLenient(true)
        localCache.put(formatString, simpleDateFormat)
        simpleDateFormat
      case Some(simpleDateFormat) => simpleDateFormat
    }
  }
}
