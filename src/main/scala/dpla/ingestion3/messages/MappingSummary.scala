package dpla.ingestion3.messages

import dpla.ingestion3.utils.Utils
import org.apache.commons.lang.StringUtils

/**
  *
  * @param shortName
  * @param runTime
  * @param attempted
  * @param mapped
  * @param warn
  * @param error
  * @param warnRecords
  * @param errorRecords
  * @param errorMsgDetails
  * @param warnMsgDetails
  */
case class MappingSummaryData(
                               shortName: String,
                               runTime: String,
                               attempted: Long,
                               mapped: Long,
                               warn: Long,
                               error: Long,
                               warnRecords: Long,
                               errorRecords: Long,
                               errorMsgDetails: String,
                               warnMsgDetails: String,
                               exceptionCount: Long
                             )
object MappingSummary {

  def centerPad(a: String, b: String, seperator: String = ".", width: Int = 80) =
    s"$a${seperator*(80-a.length-b.length)}$b"
  def center(a: String, seperator: String = " ",width: Int = 80): String =
    StringUtils.leftPad(a, (width+a.length)/2, seperator)

  /**
    * Big picutre summary in one String
    * @param data MappingSummaryData Results of individual steps (failures, successes, warnings and errors)
    * @return String Synopsis of the mapping process
    */
  def getSummary(data: MappingSummaryData): String = {
    // prettify all the digits!
    val attemptedStr = Utils.formatNumber(data.attempted)
    val mappedStr = Utils.formatNumber(data.mapped)
    val warnStr = Utils.formatNumber(data.warn)
    val errorStr = Utils.formatNumber(data.error)
    val warnRecordsStr = Utils.formatNumber(data.warnRecords)
    val errorRecordsStr = Utils.formatNumber(data.errorRecords)
    val failedCountStr = Utils.formatNumber(data.attempted-data.mapped)
    val exceptionCountStr = Utils.formatNumber(data.exceptionCount)
    val lineBreak = "-"*80

      s"""
        |$lineBreak
        |${center("Summary")}
        |
        |${centerPad("Provider", data.shortName.toUpperCase)}
        |${centerPad("Date", data.runTime)}
        |
        |${centerPad("Attempted", attemptedStr)}
        |${centerPad("Successful", mappedStr)}
        |${centerPad("Failed", failedCountStr)}
        |
        |
        |${center("Errors, Warnings and Exceptions Summary")}
        |
        |${centerPad("Warnings (messages)", warnStr)}
        |${centerPad("Warnings (records)", warnRecordsStr)}
        |
        |${centerPad("Errors (messages)", errorStr)}
        |${centerPad("Errors (records)", errorRecordsStr)}
        |
        |${centerPad("Exceptions (records)", exceptionCountStr)}
        |
        |
        |${StringUtils.leftPad("Errors and Warnings Detail (messages)", 58 ," ")}
        |
        |${if(data.warnMsgDetails.nonEmpty) "Warnings\n~~~~~~~~\n" + data.warnMsgDetails else "* No Warnings *"}
        |
        |${if(data.errorMsgDetails.nonEmpty) "Errors\n~~~~~~\n" + data.errorMsgDetails else "* No Errors *"}
        |
        |
        |${center("Better  luck next time!")}
        |$lineBreak
        |""".stripMargin
  }

}
