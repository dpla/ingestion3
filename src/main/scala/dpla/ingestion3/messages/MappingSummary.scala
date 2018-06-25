package dpla.ingestion3.messages

import dpla.ingestion3.reports.summary.{MappingSummaryData, ReportFormattingUtils}
import dpla.ingestion3.utils.Utils
import org.apache.commons.lang.StringUtils


/**
  *
  */

object MappingSummary {

  /**
    * Big picutre summary in one String
    * @param data MappingSummaryData Results of individual steps (failures, successes, warnings and errors)
    * @return String Synopsis of the mapping process
    */
  def getSummary(data: MappingSummaryData): String = {
    // prettify all the digits!
    val attemptedStr = Utils.formatNumber(data.operationSummary.recordsAttempted)
    val mappedStr = Utils.formatNumber(data.operationSummary.recordsSuccessful)
    val warnStr = Utils.formatNumber(data.messageSummary.warningCount)
    val errorStr = Utils.formatNumber(data.messageSummary.errorCount)
    val warnRecordsStr = Utils.formatNumber(data.messageSummary.warningRecordCount)
    val errorRecordsStr = Utils.formatNumber(data.messageSummary.errorRecordCount)
    val failedCountStr = Utils.formatNumber(data.operationSummary.recordsFailed)
    val exceptionCountStr = Utils.formatNumber(data.messageSummary.execeptionCount)
    val logFileMsg =
      if(data.operationSummary.logFiles.nonEmpty) data.operationSummary.logFiles.mkString("\n")
      else ""

    val lineBreak = "-"*80

      s"""
        |$lineBreak
        |${ReportFormattingUtils.center("~~~~~~~~ Mapping Summary ~~~~~~~~")}
        |
        |${ReportFormattingUtils.centerPad("Provider", data.shortName.toUpperCase)}
        |${ReportFormattingUtils.centerPad("Start date", data.timeSummary.startTime)}
        |${ReportFormattingUtils.centerPad("Runtime", data.timeSummary.runTime)}
        |
        |${ReportFormattingUtils.centerPad("Attempted", attemptedStr)}
        |${ReportFormattingUtils.centerPad("Successful", mappedStr)}
        |${ReportFormattingUtils.centerPad("Failed", failedCountStr)}
        |
        |
        |${ReportFormattingUtils.center("~~~~~~~~ Errors, Warnings and Exceptions ~~~~~~~~")}
        |
        |Messages
        |${ReportFormattingUtils.centerPad("- Errors", errorStr)}
        |${ReportFormattingUtils.centerPad("- Warnings", warnStr)}
        |
        |Records
        |${ReportFormattingUtils.centerPad("- Errors", errorRecordsStr)}
        |${ReportFormattingUtils.centerPad("- Warnings", warnRecordsStr)}
        |${ReportFormattingUtils.centerPad("- Exceptions", exceptionCountStr)}
        |
        |${ReportFormattingUtils.centerPad("Exceptions (records)", exceptionCountStr)}
        |
        |${ReportFormattingUtils.center("~~~~~~~~ Error and Warning Message Summary ~~~~~~~~")}
        |
        |${if(data.messageSummary.warningCount > 0 || data.messageSummary.errorCount > 0)
          ReportFormattingUtils.center("Error and Warning Message Summary") else ""}
        |${if(data.messageSummary.warningCount > 0)
          "\nWarnings\n" + data.messageSummary.warningMessageDetails else ""}
        |${if(data.messageSummary.errorMessageDetails.nonEmpty)
          "\nErrors\n" + data.messageSummary.errorMessageDetails else ""}
        |
        |${ReportFormattingUtils.center("~~~~~~~~ Error and Warning Message Summary ~~~~~~~~")}
        |
        |${if(logFileMsg.nonEmpty)
          ReportFormattingUtils.center("Log Files")
          logFileMsg
          }
        |
        |${ReportFormattingUtils.centerPad("Errors and Warnings Detail", exceptionCountStr)}
        |${if(data.messageSummary.warningMessageDetails.nonEmpty) "Warnings\n--------\n" + data.messageSummary.warningMessageDetails else "* No Warnings *"}
        |
        |${if(data.messageSummary.errorMessageDetails.nonEmpty) "Errors\n-------\n" + data.messageSummary.errorMessageDetails else "* No Errors *"}
        |
        |${ReportFormattingUtils.center("Log Files")}
        |
        |
        |${if(logFileMsg.nonEmpty)
          ReportFormattingUtils.center("Log Files")
          logFileMsg
          }
        |""".stripMargin
  }
}
