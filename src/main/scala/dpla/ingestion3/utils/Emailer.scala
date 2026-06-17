package dpla.ingestion3.utils

import com.amazonaws.regions.Regions
import com.amazonaws.services.simpleemail._
import com.amazonaws.services.simpleemail.model._
import dpla.ingestion3.confs.i3Conf
import net.lingala.zip4j.ZipFile
import net.lingala.zip4j.model.{ExcludeFileFilter, ZipParameters}

import java.io.{ByteArrayOutputStream, File}
import java.nio
import java.nio.file.Files
import java.util.Properties
import javax.activation.DataHandler
import javax.mail.Message.RecipientType
import javax.mail._
import javax.mail.internet.{
  InternetAddress,
  MimeBodyPart,
  MimeMessage,
  MimeMultipart
}
import javax.mail.util.ByteArrayDataSource
import scala.io.{BufferedSource, Source}

object Emailer {
  private val sender = "DPLA Bot<tech@dp.la>"

  private val prefix =
    s"""
      |This is an automated email summarizing the DPLA ingest. Please see attached ZIP file
      |for record level information about errors and warnings.
      |
      |If you have questions please contact us at <a href="mailto:tech@dp.la">tech@dp.la</a>
      |
      |- <a href="https://github.com/dpla/ingestion3/">Ingestion documentation</a>
      |- <a href="about:blank">Wikimedia project</a>
      |- <a href="about:blank">DPLA news</a>
      |""".stripMargin.split("\n")

  /**
    * Main method for CLI invocation
    *
    * Usage:
    *   Emailer <mapping-dir> <hub-name> [conf-file] [--merge-summary <path>]
    *
    * --merge-summary <path>
    *   Path to a NaraMergeUtil _SUMMARY.txt file. When provided, a "NARA Merge
    *   Statistics" section (record counts, delete stats) is prepended to the
    *   mapping summary in the email body. Intended for the nara hub only.
    */
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: Emailer <mapping-dir> <hub-name> [conf-file] [--merge-summary <path>]")
      System.err.println("Example: Emailer /path/to/mapping/dir nara")
      System.err.println("         Emailer /path/to/mapping/dir nara /path/to/i3.conf --merge-summary /path/to/harvest/_LOGS/_SUMMARY.txt")
      System.exit(1)
    }

    // Pull out --merge-summary <path> before processing positional args
    val argList = args.toList
    val mergeSummaryPath: Option[String] = {
      val idx = argList.indexOf("--merge-summary")
      if (idx >= 0 && idx + 1 < argList.length) Some(argList(idx + 1)) else None
    }

    // Pull out --email-override <address>
    val emailOverride: Option[String] = {
      val idx = argList.indexOf("--email-override")
      if (idx >= 0 && idx + 1 < argList.length) Some(argList(idx + 1)) else None
    }

    // Strip named flags and their values to get clean positional args
    val namedFlags = Set("--merge-summary", "--email-override")
    val positionalArgs: List[String] = {
      var skipNext = false
      argList.filter { a =>
        if (skipNext) { skipNext = false; false }
        else if (namedFlags.contains(a)) { skipNext = true; false }
        else true
      }
    }

    val mappingDir = positionalArgs(0)
    val hubName    = positionalArgs(1)

    // Get config file path from args or environment
    val confPath = if (positionalArgs.length >= 3) {
      positionalArgs(2)
    } else {
      Option(System.getenv("I3_CONF"))
        .getOrElse(s"${System.getProperty("user.home")}/dpla/code/ingestion3-conf/i3.conf")
    }

    // Load i3.conf using Ingestion3Conf
    val ingestion3Conf = new dpla.ingestion3.confs.Ingestion3Conf(confPath, Some(hubName))
    val conf = ingestion3Conf.load()

    // Get provider name from config
    val providerName = conf.provider.getOrElse(hubName.toUpperCase)

    // Get current month for subject line
    val currentMonth = java.time.LocalDate.now()
      .format(java.time.format.DateTimeFormatter.ofPattern("MMMM yyyy"))

    val subject = s"DPLA Ingest Summary for $providerName - $currentMonth"

    emailOverride.foreach(addr =>
      println(s"⚠️  EMAIL OVERRIDE ACTIVE — sending to $addr instead of configured contacts")
    )

    // Send email
    emailSummaryWithSubject(mappingDir, subject, conf, mergeSummaryPath, emailOverride)
  }

  /**
    * Extract and normalize email addresses from potentially malformed strings.
    * Handles formats like "Name<email@domain>" by adding missing space or extracting just the email.
    */
  private def normalizeEmails(emailString: String): Seq[String] = {
    emailString.split(',').map(_.trim).map { email =>
      // If format is "Name<email@domain>" without space, extract just the email
      val anglePattern = """.*<(.+@.+)>.*""".r
      email match {
        case anglePattern(extractedEmail) => extractedEmail.trim
        case _ => email.trim
      }
    }
  }

  /**
    * Send email summary with custom subject.
    *
    * @param mapOutput        Path to the mapping output directory (contains _SUMMARY)
    * @param subject          Email subject line
    * @param i3conf           Loaded i3.conf
    * @param mergeSummaryPath Optional path to NaraMergeUtil's _SUMMARY.txt. When
    *                         supplied, a "NARA Merge Statistics" section is prepended
    *                         to the mapping summary in the email body.
    */
  def emailSummaryWithSubject(
      mapOutput: String,
      subject: String,
      i3conf: i3Conf,
      mergeSummaryPath: Option[String] = None,
      emailOverride: Option[String] = None
  ): Unit = {
    val rawEmails = emailOverride.getOrElse(i3conf.email.getOrElse("tech@dp.la"))
    val emails = normalizeEmails(rawEmails)

    // Debug output
    println(s"Raw emails from config: $rawEmails")
    println(s"Normalized emails: ${emails.mkString(", ")}")

    val _summary    = s"$mapOutput/_SUMMARY"
    val zipped_logs = s"$mapOutput/_LOGS/logs.zip"

    val mergeSection = mergeSummaryPath
      .filter(p => new File(p).exists())
      .map(formatMergeStats)

    val body = emailBody(_summary, mergeSection)

    val attachment: Option[File] = zip(zipped_logs, mapOutput) match {
      case Some(z) => if (z.length() < 7485760) Some(z) else None
      case _       => None
    }

    send(
      recipients = emails,
      subject    = subject,
      text       = body,
      attachment = attachment
    )
  }

  /**
    * Parse a NaraMergeUtil _SUMMARY.txt and return a short plain-text section
    * reporting rows removed this cycle. Appended to the normal email body.
    */
  private def formatMergeStats(summaryPath: String): String = {
    val source = Source.fromFile(summaryPath)
    val text = try source.mkString finally source.close()

    def extract(pattern: scala.util.matching.Regex): String =
      pattern.findFirstMatchIn(text).map(_.group(1).trim).getOrElse("—")

    val newRecs    = extract("""\|\s*new:\s+([\d,]+)""".r.unanchored)
    val updated    = extract("""\|\s*update:\s+([\d,]+)""".r.unanchored)
    val delValid   = extract("""\|\s*valid deletes \(IDs in merged dataset\):\s+([\d,]+)""".r.unanchored)
    val delInvalid = extract("""\|\s*invalid deletes \(IDs not in merged dataset\):\s+([\d,]+)""".r.unanchored)
    val delInFile  = extract("""\|\s*ids to delete specified at path:\s+([\d,]+)""".r.unanchored)

    val warning = if (delValid == "0" || delValid == "—")
      "\n⚠️  Note: 0 rows were removed this cycle. If deletes were expected in this delivery, please contact us at tech@dp.la.\n"
    else ""

    s"""
       |Merge Statistics
       |----------------
       |New records added:         $newRecs
       |Records updated:           $updated
       |Records deleted:           $delValid
       |Invalid delete IDs:        $delInvalid (in delete file but not in dataset)
       |Total IDs in delete file:  $delInFile
       |$warning""".stripMargin
  }

  private lazy val suffix =
    """
      |
      |
      |Bleep bloop.
      |
      |-----------------  END  -----------------
      |
      |""".stripMargin.split("\n")

  // Only include the *.csv files in the zipped export
  private val excludeFileFilter: ExcludeFileFilter = (file: File) => {
    file.isFile & !file.getName.endsWith("csv")
  }

  private lazy val zipParameters: ZipParameters = new ZipParameters()
  zipParameters.setExcludeFileFilter(excludeFileFilter)

  def emailSummary(mapOutput: String, partner: String, i3conf: i3Conf): Unit = {
    val emails = i3conf.email.getOrElse("tech@dp.la").split(',')

    val _summary = s"$mapOutput/_SUMMARY"
    val zipped_logs =
      s"$mapOutput/_LOGS/logs.zip" // FIXME provider-date-mapping-logs.zip

    val body = emailBody(_summary)

    val attachment: Option[File] = zip(zipped_logs, mapOutput) match {
      case Some(z) => if (z.length() < 7485760) Some(z) else None
      case _       => None
    }

    send(
      recipients = emails,
      subject =
        s"DPLA Ingest Summary for $partner", // FIXME Add current month in subject
      text = body,
      attachment = attachment
    )
  }

  private def emailBody(summaryFile: String, mergeSection: Option[String] = None): String = {
    // READ in _SUMMARY file
    val source: BufferedSource = Source.fromFile(summaryFile)
    val lines =
      try {
        source.getLines().toList
      } finally {
        source.close
      }
    // The last five lines of the mapping _SUMMARY reference local log file paths
    // that are meaningless to external recipients, so we drop them.
    // The merge delete stats section (NARA only) is appended after the mapping
    // summary so the normal output comes first, with the extra stats at the end.
    val mergePart = mergeSection.map(s => List(s)).getOrElse(Nil)
    List
      .concat(List("<pre>"), prefix, lines.dropRight(5), mergePart, suffix, List("</pre>"))
      .mkString("\n")
  }

  private def zip(zippedLogs: String, mapOutput: String): Option[File] = {
    val errors = new File(s"$mapOutput/_LOGS/errors/")
    // Only create the zip if there are actual CSV error files — an empty errors
    // directory (or no directory at all) means there's nothing worth attaching.
    val csvFiles = Option(errors.listFiles()).getOrElse(Array.empty)
      .filter(f => f.isFile && f.getName.endsWith(".csv"))
    if (csvFiles.nonEmpty) {
      val zipOut = new ZipFile(zippedLogs)
      zipOut.addFolder(errors, zipParameters)
      zipOut.close()
      Some(new File(zippedLogs))
    } else {
      None
    }
  }

  // Body must be plain text - HTML markup would require a `withHtml` call, not a `withText` call.
  private def send(
      recipients: Seq[String],
      subject: String,
      text: String,
      attachment: Option[File]
  ): Unit = {
    // Message builder to add attachments
    // https://docs.aws.amazon.com/ses/latest/dg/example_ses_SendEmail_section.html

    try {
      // Get AWS region from environment variable, system property, or default to us-east-1
      val awsRegion = Option(System.getenv("AWS_REGION"))
        .orElse(Option(System.getProperty("aws.region")))
        .getOrElse("us-east-1")

      val client = AmazonSimpleEmailServiceClientBuilder
        .standard()
        .withRegion(Regions.fromName(awsRegion))
        .build()

      val session = Session.getDefaultInstance(new Properties())
      val message = new MimeMessage(session)

      message.setSubject(subject)

      val sendTo: Array[Address] =
        recipients.toArray.map(new InternetAddress(_))
      val replyTo: Array[Address] = Array(new InternetAddress(sender))
      val sendFrom: Address = new InternetAddress(sender)
      val sendCc: Array[Address] = Array[Address](
        new InternetAddress("tech@dp.la")
      )

      message.setFrom(sendFrom)
      message.setReplyTo(replyTo)
      message.setRecipients(RecipientType.TO, sendTo)
      message.setRecipients(RecipientType.CC, sendCc)

      val messageBody = new MimeMultipart("alternative")
      val wrap = new MimeBodyPart()

      val htmlPart = new MimeBodyPart()
      htmlPart.setText(text, "utf-8", "html")
      messageBody.addBodyPart(htmlPart)

      // TODO text part
      wrap.setContent(messageBody)

      val msg = new MimeMultipart("mixed")
      message.setContent(msg)
      msg.addBodyPart(wrap)

      // Define the attachment if small enough
      if (attachment.isDefined) {
        val fileBytes = Files.readAllBytes(attachment.get.toPath)
        val att = new MimeBodyPart()
        val fds = new ByteArrayDataSource(fileBytes, "application/zip;")
        att.setDataHandler(new DataHandler(fds))
        val reportName: String = "log_file.zip"
        att.setFileName(reportName)
        msg.addBodyPart(att)
      }

      val bos = new ByteArrayOutputStream()
      message.writeTo(bos)
      val bb = nio.ByteBuffer.wrap(bos.toByteArray)
      val rawMessage = new RawMessage().withData(bb)

      val sendRawEmailRequest =
        new SendRawEmailRequest().withRawMessage(rawMessage)
      client.sendRawEmail(sendRawEmailRequest)

      System.out.println(s"Email sent to ${recipients.mkString(", ")}")
    } catch {
      case ex: Exception =>
        throw ex
    }
  }
}
