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
                               warnMsgDetails: String
                             )
object MappingSummary {

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

    val lineBreak = "-"*80
    // report.
      s"""
        |$lineBreak
        |${StringUtils.leftPad("Summary", 43, " ")}
        |$lineBreak
        |Provider: ${data.shortName.toUpperCase}
        |Date:     ${data.runTime}
        |
        |Attempted to map:     $attemptedStr
        |Successfully mapped:  $mappedStr
        |                      ${"-"*attemptedStr.length}
        |Failed:               ${Utils.formatNumber(data.attempted-data.mapped)}
        |
        |
        |$lineBreak
        |${StringUtils.leftPad("Errors and Warnings Summary", 53 ," ")}
        |$lineBreak
        |Warnings (messages):  $warnStr
        |Warnings (records):   $warnRecordsStr
        |
        |Errors (messages):    $errorStr
        |Errors (records):     $errorRecordsStr
        |
        |
        |$lineBreak
        |${StringUtils.leftPad("Errors and Warnings Detail (messages)", 58 ," ")}
        |$lineBreak
        |WARNINGS
        |--------
        |${if(data.warnMsgDetails.nonEmpty) data.warnMsgDetails else "* No Warnings *"}
        |
        |ERRORS
        |------
        |${if(data.errorMsgDetails.nonEmpty) data.errorMsgDetails else "* No Errors *"}
        |
        |      Better luck next time!
        |${"-"*80}
        |""".stripMargin


  }

}
