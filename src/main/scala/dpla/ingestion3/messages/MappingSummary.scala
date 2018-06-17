package dpla.ingestion3.messages

import dpla.ingestion3.utils.Utils

case class MappingSummaryData(
                               shortName: String,
                               runTime: String,
                               attempted: Long,
                               mapped: Long,
                               warn: Long,
                               error: Long,
                               warnRecords: Long,
                               errorRecords: Long
                               )
object MappingSummary {

  def getSummary(data: MappingSummaryData): String = {

    // prettify all the digits!
    val attemptedStr = Utils.formatNumber(data.attempted)
    val mappedStr = Utils.formatNumber(data.mapped)
    val warnStr = Utils.formatNumber(data.warn)
    val errorStr = Utils.formatNumber(data.error)
    val warnRecordsStr = Utils.formatNumber(data.warnRecords)
    val errorRecordsStr = Utils.formatNumber(data.errorRecords)

    // report.
      s"""
        |${"-"*80}
        |Mapping report for ${data.shortName.toUpperCase}.
        |Ran on ${data.runTime.replaceFirst(" ", " at ")}
        |
        |Attempted to map:     $attemptedStr
        |Successfully mapped:  $mappedStr
        |                      ${"-"*attemptedStr.length}
        |Failures:             ${Utils.formatNumber(data.attempted-data.mapped)}
        |
        |
        |---------------------------
        |Errors and Warnings Summary
        |---------------------------
        |Warnings (messages):  $warnStr
        |- Records             $warnRecordsStr
        |
        |Errors (messages):    $errorStr
        |- Records             $errorRecordsStr
        |
        |
        |---------------------------
        |Errors and Warnings Detail   (FAKE)
        |---------------------------
        |WARN
        |----
        |  Unable to mint URI
        |    - preview         150
        |    - edmRights       50
        |  Field too long
        |    - description     5,000
        |
        |ERROR
        |-----
        |  Unable to mint URI
        |    - isShownAt       12
        |
        |
        |-------------
        |TROUBLEMAKERS  (FAKE)
        |-------------
        |  identifier_xyz
        |    - WARN preview
        |    - ERROR isShownAt
        |    - ERROR rights
        |
        |
        |
        |Better luck next time!
        |${"-"*80}
        |""".stripMargin


  }

}
