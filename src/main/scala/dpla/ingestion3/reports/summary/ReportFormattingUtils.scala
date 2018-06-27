package dpla.ingestion3.reports.summary

import org.apache.commons.lang.StringUtils

/**
  *
  */
object ReportFormattingUtils {
  def centerPad(a: String, b: String, seperator: String = ".", width: Int = 80) =
    s"$a${seperator*(80-a.length-b.length)}$b"

  def center(a: String, seperator: String = " ",width: Int = 80): String =
    StringUtils.leftPad(a, (width+a.length)/2, seperator)
}
