package dpla.ingestion3.harvesters.oai

/** The information that was used to create a request */
case class OaiRequestInfo(
    verb: String,
    metadataPrefix: Option[String],
    set: Option[String],
    resumptionToken: Option[String],
    timestamp: Long,
    cursor: Option[Int] = None,
    completeListSize: Option[Int] = None
)

/** A single record from an OAI Harvest. */
case class OaiRecord(id: String, document: String, info: OaiRequestInfo)

/** A single set from an OAI harvest. */
case class OaiSet(id: String, document: String, info: OaiRequestInfo)

/** Represents a single response from an OAI harvest. */
sealed trait OaiResponse

/** A single page response from an Oai feed. The page may include an error
  * message.
  */
case class OaiPage(page: String, info: OaiRequestInfo) extends OaiResponse

trait OaiError extends OaiResponse

object OaiError {
  def errorForCode(code: String, info: OaiRequestInfo): OaiError = code match {
    case "badArgument"             => BadArgument(info)
    case "badResumptionToken"      => BadResumptionToken(info)
    case "badVerb"                 => BadVerb(info)
    case "cannotDisseminateFormat" => CannotDisseminateFormat(info)
    case "idDoesNotExist"          => IdDoesNotExist(info)
    case "noRecordsMatch"          => NoRecordsMatch(info)
    case "noMetadataFormats"       => NoMetadataFormats(info)
    case "noSetHierarchy"          => NoSetHierarchy(info)
    case _ => throw new RuntimeException("No error for code: " + code)
  }
}

case class BadArgument(info: OaiRequestInfo) extends OaiError

case class BadResumptionToken(info: OaiRequestInfo) extends OaiError

case class BadVerb(info: OaiRequestInfo) extends OaiError

case class CannotDisseminateFormat(info: OaiRequestInfo) extends OaiError

case class IdDoesNotExist(info: OaiRequestInfo) extends OaiError

case class NoRecordsMatch(info: OaiRequestInfo) extends OaiError

case class NoMetadataFormats(info: OaiRequestInfo) extends OaiError

case class NoSetHierarchy(info: OaiRequestInfo) extends OaiError

/** Rich exception for OAI harvest failures that carries full request context.
  *
  * @param requestInfo
  *   OAI request metadata (set, token, cursor, etc.)
  * @param url
  *   The URL that was requested
  * @param stage
  *   Where in the pipeline the failure occurred
  * @param firstId
  *   Best-effort first record identifier from the raw page
  * @param lastId
  *   Best-effort last record identifier from the raw page
  * @param responseSnippet
  *   Optional first N characters of the raw response
  * @param oaiError
  *   When set (e.g. stage oai_error), message is formatted for non-developers
  * @param cause
  *   The underlying exception
  */
class OaiHarvestException(
    val requestInfo: OaiRequestInfo,
    val url: String,
    val stage: String,
    val firstId: Option[String] = None,
    val lastId: Option[String] = None,
    val responseSnippet: Option[String] = None,
    val oaiError: Option[OaiError] = None,
    cause: Throwable
) extends RuntimeException(
      OaiHarvestException.buildMessage(
        requestInfo,
        url,
        stage,
        firstId,
        lastId,
        oaiError,
        cause
      ),
      cause
    )

object OaiHarvestException {

  /** Human-readable labels for OAI error notification (no Scala/class names).
    */
  def formatOaiErrorForDisplay(
      oaiError: OaiError,
      info: OaiRequestInfo,
      cause: Throwable,
      originalMessage: String
  ): String = {
    val sb = new StringBuilder
    sb.append(s"  Cause: ${cause.getClass.getSimpleName}: OAI Error\n")
    sb.append(s"  OAI Error: ${oaiError.getClass.getSimpleName}\n")
    sb.append(s"  MetadataPrefix: ${info.metadataPrefix.getOrElse("-")}\n")
    sb.append(s"  OAI Set: ${info.set.getOrElse("-")}\n")
    sb.append(s"  Resumption Token: ${info.resumptionToken.getOrElse("-")}\n")
    if (info.cursor.isDefined || info.completeListSize.isDefined) {
      val cursor = info.cursor.fold("-")(_.toString)
      val total = info.completeListSize.fold("-")(_.toString)
      sb.append(s"  Cursor: $cursor\n")
      sb.append(s"  Complete List Size: $total\n")
    }
    sb.append(s"  OriginalMessage: $originalMessage")
    sb.toString
  }

  def buildMessage(
      info: OaiRequestInfo,
      url: String,
      stage: String,
      firstId: Option[String],
      lastId: Option[String],
      oaiError: Option[OaiError],
      cause: Throwable
  ): String = {
    val sb = new StringBuilder
    sb.append(s"OAI harvest error at stage $stage\n")
    info.set.foreach(s => sb.append(s"  Set: $s\n"))
    info.resumptionToken.foreach(t => sb.append(s"  Resumption token: $t\n"))
    info.cursor.foreach(c => sb.append(s"  Cursor: $c"))
    info.completeListSize.foreach(t => sb.append(s" / $t"))
    if (info.cursor.isDefined || info.completeListSize.isDefined)
      sb.append("\n")
    sb.append(s"  URL: $url\n")
    firstId.foreach(f => sb.append(s"  First identifier: $f\n"))
    lastId.foreach(l => sb.append(s"  Last identifier: $l\n"))
    oaiError match {
      case Some(err) =>
        sb.append(formatOaiErrorForDisplay(err, info, cause, cause.getMessage))
      case None =>
        sb.append(
          s"  Cause: ${cause.getClass.getSimpleName}: ${cause.getMessage}"
        )
    }
    sb.toString
  }

  /** Format a Slack-friendly notification message. */
  def formatForSlack(
      info: OaiRequestInfo,
      url: String,
      stage: String,
      firstId: Option[String],
      lastId: Option[String],
      cause: Throwable
  ): String = {
    val sb = new StringBuilder
    sb.append(s"*OAI debug context*\n")
    info.set.foreach(s => sb.append(s"• Set: `$s`\n"))
    info.resumptionToken.foreach(t => sb.append(s"• Resumption token: `$t`\n"))
    (info.cursor, info.completeListSize) match {
      case (Some(c), Some(t)) => sb.append(s"• Cursor: $c / $t\n")
      case (Some(c), None)    => sb.append(s"• Cursor: $c\n")
      case _                  =>
    }
    sb.append(s"• URL: $url\n")
    firstId.foreach(f => sb.append(s"• First ID: `$f`\n"))
    lastId.foreach(l => sb.append(s"• Last ID: `$l`\n"))
    sb.append(
      s"\n*Error*: ${cause.getClass.getSimpleName}: ${cause.getMessage}"
    )
    sb.toString
  }

  /** Format a plain-text notification message (for email). */
  def formatForEmail(
      info: OaiRequestInfo,
      url: String,
      stage: String,
      firstId: Option[String],
      lastId: Option[String],
      cause: Throwable
  ): String = {
    val sb = new StringBuilder
    sb.append(s"OAI debug context:\n")
    info.set.foreach(s => sb.append(s"- Set: $s\n"))
    info.resumptionToken.foreach(t => sb.append(s"- Resumption token: $t\n"))
    (info.cursor, info.completeListSize) match {
      case (Some(c), Some(t)) => sb.append(s"- Cursor: $c / $t\n")
      case (Some(c), None)    => sb.append(s"- Cursor: $c\n")
      case _                  =>
    }
    sb.append(s"- URL: $url\n")
    firstId.foreach(f => sb.append(s"- First ID: $f\n"))
    lastId.foreach(l => sb.append(s"- Last ID: $l\n"))
    sb.append(
      s"\nFull error: ${cause.getClass.getSimpleName}: ${cause.getMessage}"
    )
    sb.toString
  }

  /** Build the complete email body for an OAI harvest failure notification.
    *
    * This is the single source of truth for the email content sent to
    * tech@dp.la. The body is built entirely in Scala and passed through the
    * shell/Python notification scripts without modification.
    */
  def buildEmailBody(hubName: String, ex: OaiHarvestException): String = {
    val sb = new StringBuilder
    sb.append("DPLA OAI Harvest Failure\n\n")
    sb.append(s"Hub: $hubName\n\n")
    sb.append("Error:\n")
    sb.append(ex.getMessage)
    sb.append("\n\n---\nIf you have questions, contact tech@dp.la.\n")
    sb.toString
  }

  /** Build a generic email body for non-OAI harvest failures. */
  def buildGenericEmailBody(hubName: String, error: Throwable): String = {
    val sb = new StringBuilder
    sb.append("DPLA Harvest Failure\n\n")
    sb.append(s"Hub: $hubName\n\n")
    sb.append("Error:\n")
    sb.append(s"${error.getClass.getSimpleName}: ${error.getMessage}")
    sb.append("\n\n---\nIf you have questions, contact tech@dp.la.\n")
    sb.toString
  }
}
