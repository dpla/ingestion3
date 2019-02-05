package dpla.ingestion3.reports.summary

case class TimeSummary(
                        startTime: String = "",
                        endTime: String = "",
                        runTime: String= ""
                      )

case class OperationSummary(
                             recordsAttempted: Long = -1,
                             recordsSuccessful: Long = -1,
                             recordsFailed: Long = -1,
                             logFiles: Seq[String] = Seq()
                           )

case class MessageSummary(
                           errorCount: Long = -1, // number of errors (message and exception)
                           warningCount: Long = -1,   // message warning
                           errorRecordCount: Long = -1, // 1:1 if exception 1:n if from message
                           warningRecordCount: Long = -1, // 1:n a record can raise many warnings
                           errorMessageDetails: String = "", // error messages
                           warningMessageDetails: String  = "", // warning messages
                           duplicateOriginalIds: Long = -1 // number of records with duplicate original IDs
                         )



case class MappingSummaryData(
                               shortName: String = "",
                               operationSummary: OperationSummary,
                               timeSummary: TimeSummary,
                               messageSummary: MessageSummary
                             )

case class EnrichmentSummaryData(
                                  shortName: String,
                                  operationSummary: OperationSummary,
                                  timeSummary: TimeSummary,
                                  enrichmentOpSummary: EnrichmentOpsSummary
                                )

case class EnrichmentOpsSummary(
                                 typeImproved: Long,
                                 dateImproved: Long,
                                 langImproved: Long,
                                 placeImprove: Long,
                                 langSummary: String,
                                 typeSummary: String,
                                 placeSummary: String,
                                 dateSummary: String
                               )
