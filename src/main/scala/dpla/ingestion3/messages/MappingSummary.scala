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
    val logFileMsg = data.operationSummary.logFiles.mkString("\n")

    val lineBreak = "-"*80

      s"""
        |$lineBreak
        |${ReportFormattingUtils.center("Mapping Summary")}
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
        |${ReportFormattingUtils.center("Errors, Warnings and Exceptions")}
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
        |
        |${StringUtils.leftPad("Error and Warning Message Summary", 58 ," ")}
        |
        |${if(data.messageSummary.warningMessageDetails.nonEmpty)
          "Warnings\n~~~~~~~~\n" + data.messageSummary.warningMessageDetails else "* No Warnings *"}
        |
        |${if(data.messageSummary.errorMessageDetails.nonEmpty)
          "Errors\n~~~~~~\n" + data.messageSummary.errorMessageDetails else "* No Errors *"}
        |
        |
        |${StringUtils.leftPad("Log Files", 45 ," ")}
        |
        |${logFileMsg}
        |
        |""".stripMargin
  }

}
