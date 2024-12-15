package dpla.ingestion3.utils

import scala.util.matching.Regex

object EDTFDate {

  private val yearPat4 = """-?\d{1,4}"""
  private val yearPatLong = """y-?\d{5,}"""
  // monthPat and dayPat try to provide a little insurance against impossible
  // dates, but do not verify whether the month actually has a particular
  // day, like 2017-11-31 or 2017-02-29, which are both invalid.
  private val monthPat = "(?:0[1-9]|1[0-2])"
  private val dayPat = "(?:0[1-9]|[12][0-9]|3[01])"
  private val monthAndDayPat = s"-?(?:$monthPat?-?$dayPat?|2[1-4])" // incl. season
  private val basicYmdPat = s"(?:$yearPat4|$yearPatLong)$monthAndDayPat"
  private val unspecDatePat = """(?:\d{3}u|\d{2}uu|\d{4}(?:-uu){1,2}|\d{4}-\d{2}-uu)"""
  private val fullYmdPat = s"(?:$basicYmdPat|$unspecDatePat)"

  /*
   * dateRegex has one match group, for the date. We allow for "uncertain /
   * approximate" symbols at the end of the string, but don't return them
   * with our range values.
   */
  private val dateRegex: Regex =
    ("^(" + // start capture
      fullYmdPat +
      ")" + // end capture
      """(?:[?~]+)?$""" // optional "~" and "?"
    ).r

  /*
   * dateAndTimeRegex captures the date part of the timestamp (YYYY-mm-dd)
   * because it's assumed that edm:TimeSpan's 'begin' and 'end' are just
   * supposed to be dates, without time. If that's wrong, change this and
   * rangeForDateAndTime().
   */
  private val dateAndTimeRegex: Regex =
    """^(\d{4}-\d{2}-\d{2})T\d{2}:\d{2}:\d{2}(?:Z|[\+\-]\d{2}:\d{2})?$""".r

  /*
   * There are two capture groups in intervalRegex, one for the beginning
   * and one for the end.
   */
  private val intervalRegex: Regex =
    ("^(" + // start capture 1
      "(?:" + // start grouping for "unknown"
      fullYmdPat +
      "|unknown" +
      ")" + // end grouping for "unknown"
      ")" + // end capture 1
      """(?:[?~]+)?""" + // optional "~" and "?"
      "/" + // forward slash delimiter
      "(" + // start capture 2
      "(?:" + // start grouping for "unknown"
      fullYmdPat +
      "|unknown" +
      ")" + // end grouping for "unknown"
      ")" + // end capture 2
      """(?:[?~]+)?""" + // optional "~" and "?"
      """$""").r

  /*
   * extendedIntervalRegex deals with "open" dates in the Level 1
   * spec.
   */
  private val openIntervalRegex: Regex =
    ("^(" + // start capture
      fullYmdPat +
      ")" + // end capture
      """/open$""").r

  private def clean(s: String): String = {
    s.replaceFirst("y", "")
      .replaceFirst("unknown", "")
      .replaceFirst("""(\d{4})-2[1-4]""", """$1""") // strip season
  }

  /** Return a range that makes sense for an "unspecified" pattern
    *
    * @param s
    *   The string to consider
    * @return
    *   Tuple of Strings (date, date)
    */
  private def rangeForUnspecDate(s: String): DateRangeStrings = {
    s match {
      case x if x matches """^\d{3}u$""" =>
        DateRangeStrings(x.replaceFirst("u", "0"), x.replaceFirst("u", "9"))
      case x if x matches """^\d{2}uu$""" =>
        DateRangeStrings(x.replaceFirst("uu", "00"), x.replaceFirst("uu", "99"))
      case x if x matches """^\d{4}-\d{2}-uu$""" =>
        val date = x.replaceAll("""^(\d{4}-\d{2})-uu$""", """$1""")
        DateRangeStrings(date, date)
      case x if x matches """^\d{4}-uu-uu$""" =>
        DateRangeStrings(
          x.replaceAll("""^(\d{4})-uu-uu$""", """$1-01-01"""),
          x.replaceAll("""^(\d{4})-uu-uu$""", """$1-12-31""")
        )
      case _ => DateRangeStrings("", "")
    }
  }

  /** Return begin and end date range for an exact EDTF date, if matched
    *
    * @param s
    *   The string to consider
    * @return
    *   Optional tuple of Strings (begin, end)
    * @see
    *   dpla.ingestion3.utils.EDTFDate.dateRegex
    * @see
    *   5.1.1 (Date) at
    *   https://www.loc.gov/standards/datetime/pre-submission.html
    */
  def rangeForExactDate(s: String): Option[DateRangeStrings] = {
    dateRegex.findFirstMatchIn(s) match {
      case Some(matched) =>
        if (matched.group(1).contains("u")) {
          Some(rangeForUnspecDate(matched.group(1)))
        } else {
          Some(
            DateRangeStrings(clean(matched.group(1)), clean(matched.group(1)))
          )
        }
      case None => None
    }
  }

  /** Return begin and end date range for a timestamp, if matched
    *
    * @param s
    *   The string to consider
    * @return
    *   Optional tuple of Strings (begin, end)
    * @see
    *   dpla.ingestion3.utils.EDTFDate.dateAndTimeRegex
    * @see
    *   5.1.2 (Date and Time) at
    *   https://www.loc.gov/standards/datetime/pre-submission.html
    */
  def rangeForDateAndTime(s: String): Option[DateRangeStrings] = {
    dateAndTimeRegex.findFirstMatchIn(s) match {
      case Some(matched) =>
        Some(DateRangeStrings(matched.group(1), matched.group(1)))
      case None => None
    }
  }

  /** Return begin and end date range for an interval, if matched
    *
    * Dates given as "unknown" will be represented as empty strings.
    *
    * @param s
    *   The string to consider
    * @return
    *   Optional tuple of Strings (begin, end)
    * @see
    *   dpla.ingestion3.utils.EDTFDate.intervalRegex
    * @see
    *   5.1.3 (Interval) and 5.2.3 (Extended Interval) at
    *   https://www.loc.gov/standards/datetime/pre-submission.html
    */
  def rangeForInterval(s: String): Option[DateRangeStrings] = {
    intervalRegex.findFirstMatchIn(s) match {
      case Some(matched) =>
        Some(DateRangeStrings(clean(matched.group(1)), clean(matched.group(2))))
      case None => None
    }
  }

  /** Return begin date and "" for open end date, if "open" string is matched
    *
    * @param s
    *   The string to consider
    * @return
    *   Optional tuple of Strings (begin, "")
    * @see
    *   dpla.ingestion3.utils.EDTFDate.openIntervalRegex
    * @see
    *   5.2.3 (Extended Interval) at
    *   https://www.loc.gov/standards/datetime/pre-submission.html
    */
  def rangeForOpenInterval(s: String): Option[DateRangeStrings] = {
    openIntervalRegex.findFirstMatchIn(s) match {
      case Some(matched) =>
        Some(DateRangeStrings(clean(matched.group(1)), ""))
      case None => None
    }
  }

  /** Return begin and end date range for an EDTF string, if matched
    *
    * @param s
    *   The string to consider
    * @return
    *   Optional tuple of Strings (begin, end)
    * @see
    *   https://www.loc.gov/standards/datetime/pre-submission.html
    */
  def rangeForEDTF(s: String): Option[DateRangeStrings] = {
    rangeForExactDate(s) match {
      case Some(rv) => Some(rv)
      case None =>
        rangeForDateAndTime(s) match {
          case Some(rv) => Some(rv)
          case None =>
            rangeForInterval(s) match {
              case Some(rv) => Some(rv)
              case None =>
                rangeForOpenInterval(s) match {
                  case Some(rv) => Some(rv)
                  case None     => None
                }
            }
        }
    }
  }

  case class DateRangeStrings(begin: String, end: String)
}
