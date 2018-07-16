package dpla.ingestion3.messages

import dpla.ingestion3.reports.summary.{EnrichmentSummaryData, ReportFormattingUtils}
import dpla.ingestion3.utils.Utils



object EnrichmentSummary {

  def getSummary(data: EnrichmentSummaryData): String = {
    // prettify all the digits!
    val attemptedStr = Utils.formatNumber(data.operationSummary.recordsAttempted)
    val recordsImproved = Utils.formatNumber(data.operationSummary.recordsSuccessful)
    val recordsUnimproved = Utils.formatNumber(data.operationSummary.recordsAttempted-data.operationSummary.recordsSuccessful)

    val typeImproved = Utils.formatNumber(data.enrichmentOpSummary.typeImproved)
    val dateImproved = Utils.formatNumber(data.enrichmentOpSummary.dateImproved)
    val langImproved = Utils.formatNumber(data.enrichmentOpSummary.langImproved)
    val placeImproved = Utils.formatNumber(data.enrichmentOpSummary.placeImprove)

    val failedCountStr = Utils.formatNumber(data.operationSummary.recordsFailed)
    val lineBreak = "-"*80

    s"""
       |
       |${ReportFormattingUtils.center("Enrichment Summary")}
       |
       |${ReportFormattingUtils.centerPad("Provider", data.shortName.toUpperCase)}
       |${ReportFormattingUtils.centerPad("Start date", data.timeSummary.startTime)}
       |${ReportFormattingUtils.centerPad("Runtime", data.timeSummary.runTime)}
       |
       |${ReportFormattingUtils.centerPad("Attempted", attemptedStr)}
       |${ReportFormattingUtils.centerPad("Improved", recordsImproved)}
       |${ReportFormattingUtils.centerPad("Unimproved", recordsUnimproved)}
       |
       |${ReportFormattingUtils.center("Field Improvements")}
       |${ReportFormattingUtils.centerPad("Type", typeImproved)}
       |${data.enrichmentOpSummary.typeSummary}
       |${ReportFormattingUtils.centerPad("Language", langImproved)}
       |${data.enrichmentOpSummary.langSummary}
       |
       |${ReportFormattingUtils.centerPad("Place", placeImproved)}
       |${ReportFormattingUtils.centerPad("Date", dateImproved)}
       |
       |${ReportFormattingUtils.center("Log Files")}
       |${data.operationSummary.logFiles.mkString("\n")}
       |
       |
       |${ReportFormattingUtils.center("Better  luck next time!")}
       |$lineBreak
       |""".stripMargin
  }
}

