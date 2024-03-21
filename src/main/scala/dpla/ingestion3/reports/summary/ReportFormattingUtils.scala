package dpla.ingestion3.reports.summary

import org.apache.commons.lang3.StringUtils

/** Provides utility functions for formatting strings in a report of ingestion
  * operations.
  */
object ReportFormattingUtils {

  /** Pads two strings with a delimiter out to the specified width (default is
    * 80) If length of (a + b + 2) > width then the b string is truncated down
    * to a length such that a + 2 + b = width
    *
    * Ex.
    * errors......./Users/scott/i3/cdl/mapping/20181004_93223/_LOGS/mapping-errors.log
    * warnings..ers/scott/i3/cdl/mapping/20181004_93223/_LOGS/mapping-cdl-warnings.log
    *
    * @param a
    *   First string
    * @param b
    *   Second string
    * @param separator
    *   Character to separate strings a and b with
    * @param width
    *   Max string width
    * @return
    */
  def centerPad(
      a: String,
      b: String,
      separator: String = ".",
      width: Int = 80
  ): String = {
    val limit = if (a.length + b.length + 2 > width) {
      width - (a.length + 2) // limit string b
    } else {
      b.length // show all of string b
    }
    s"$a${separator * (width - a.length - limit)}${b.takeRight(limit)}"
  }

  /** Returns a new String with the provided value centered using whitespace (by
    * default)
    *
    * Ex. If given 'Errors' it will return: ' Errors '
    *
    * @param a
    *   String to center
    * @param seperator
    *   Value to add before and after the string, default ' '
    * @param width
    *   With to center on, default 80
    * @return
    *   String with the provided string centered
    */
  def center(a: String, seperator: String = " ", width: Int = 80): String =
    StringUtils.leftPad(a, (width + a.length) / 2, seperator)
}
