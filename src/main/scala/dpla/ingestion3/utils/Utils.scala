package dpla.ingestion3.utils

import java.io.{File, PrintWriter}
import java.net.{URI, URL}
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.Calendar

import dpla.ingestion3.confs.i3Conf
import org.apache.commons.lang.StringUtils
import org.apache.log4j.{FileAppender, LogManager, Logger, PatternLayout}
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.json4s.JValue
import org.json4s.jackson.JsonMethods._

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.xml.NodeSeq



object Utils {

  /**
    * Count the number of files in the given directory, outDir.
    *
    * @param outDir Directory to count
    * @param ext    File extension to filter by
    * @return The number of files that match the extension
    */
  def countFiles(outDir: File, ext: String): Long = {
    outDir.list()
      .par
      .count(fileName => fileName.endsWith(ext))
  }

  /**
    * Creates and returns a logger object
    *
    * @param operation Name of operation to log
    * @param shortName Provider short name
    * @return
    */
  def createLogger(operation: String, shortName: String = ""): Logger = {
    val logger: Logger = LogManager.getLogger("ingestion3")
    val appender = Utils.getFileAppender(shortName, operation)
    logger.addAppender(appender)
    logger
  }

  /**
    * Delete a directory
    * Taken from http://stackoverflow.com/questions/25999255/delete-directory-recursively-in-scala#25999465
    *
    * @param file File or directory to delete
    */
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  /**
    * Prettify JSON
    *
    * @param data JSON
    * @return Formatted JSON string
    */
  def formatJson(data: JValue): String = pretty(render(data))

  /**
    * Format numbers with commas
    *
    * @param n A number
    * @return xxx,xxx
    */
  def formatNumber(n: Long): String = {
    val formatter = java.text.NumberFormat.getIntegerInstance
    formatter.format(n)
  }

  /**
    * Formats runtime
    *
    * @param runtime Runtime in milliseconds
    * @return Runtime formatted as MM:ss
    */
  def formatRuntime(runtime: Long): String = {
    val runDuration = Duration.create(runtime, MILLISECONDS)
    val hr = StringUtils.leftPad(runDuration.toHours.toString, 2, "0")
    val min = StringUtils.leftPad((runDuration.toMinutes % 60).round.toString, 2, "0")
    val sec = StringUtils.leftPad((runDuration.toSeconds % 60).round.toString, 2, "0")
    val ms = StringUtils.rightPad((runDuration.toMillis % 1000).round.toString, 3, "0")

    s"$hr:$min:$sec.$ms"
  }

  /**
    * Formats time given in ms since epoch as 'MM/dd/yyyy HH:mm:ss'
    *
    * @param currentTimeInMs Long
    * @return
    */
  def formatDateTime(currentTimeInMs: Long): String = {
    val instant = Instant.ofEpochMilli(currentTimeInMs)
    val dtUtc = ZonedDateTime.ofInstant(instant, ZoneId.of("America/New_York"))
    val dtFormatter = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss")
    dtFormatter.format(dtUtc)
  }

  /**
    * Formats the Node in a more human-readable form
    *
    * @param xml An XML node
    * @return Formatted String representation of the node
    */
  def formatXml(xml: NodeSeq): String = {
    val prettyPrinter = new scala.xml.PrettyPrinter(80, 2)
    prettyPrinter.format(xml.head).toString
  }

  /**
    * Uses runtime information to create a log4j file appender.
    *
    * @param provider - Name partner
    * @param process  - Process name [harvest, mapping, enrichment]
    * @return FileAppender
    */
  def getFileAppender(provider: String, process: String): FileAppender = {
    val layout = new PatternLayout()
    layout.setConversionPattern("[%p] %d %c %M - %m%n")

    // Date formatting for the file name
    val format = new SimpleDateFormat("y-M-d")
    val date = format.format(Calendar.getInstance().getTime)

    new FileAppender(
      layout,
      s"log/$provider-$process-$date.log",
      true)
  }

  /**
    * Sorts the contents of the given path to find the most recent folder
    * within the provided path that ends with '.avro'
    *
    * @return Option[String] Absolute path to the most recent data within folder
    *
    */
  def getMostRecent(path: String): Option[String] = {
    val rootFile = new File(path)

    rootFile
      .listFiles()
      .filter(f => f.getName.endsWith(".avro"))
      .map(f => f.getAbsolutePath)
      .sorted
      .lastOption
  }

  // TODO These *Summary methods should be refactored and normalized when we fixup logging
  /**
    * Print the results of an activity
    *
    * Example:
    *
    * Record count: 242,924
    * Runtime: 4:24
    * Throughput: 920 records per second
    *
    * @param runtime     Runtime in milliseconds
    * @param recordCount Number of records output
    */
  def harvestSummary(out: String, runtime: Long, recordCount: Long): String = {
    val recordsPerSecond: Long = recordCount / (runtime / 1000)

    s"\n\nSaved to: $out\n" +
      s"Record count: ${Utils.formatNumber(recordCount)}\n" +
      s"Runtime: ${formatRuntime(runtime)}\n" +
      s"Throughput: ${Utils.formatNumber(recordsPerSecond)} records per second"
  }

  /**
    * Tries to create a URL object from the string
    *
    * @param url String url
    * @return True if a URL object can be made from url
    *         False if it fails (malformed url, invalid characters, not a url, empty string)
    */
  def isUrl(url: String): Boolean = url.trim.nonEmpty && Try {
    new URL(url)
  }.isSuccess

  /**
    * Print mapping summary information
    *
    * @param harvestCount Number of harvested records
    * @param mapCount     Number of mapped records
    * @param errors       Number of mapping failures
    * @param outDir       Location to save mapping output
    * @param shortName    Provider short name
    */
  def mappingSummary(harvestCount: Long,
                     mapCount: Long,
                     failureCount: Long,
                     errors: Array[String],
                     outDir: String,
                     shortName: String,
                     logger: Logger): Unit = {
    val logDir = new File(s"$outDir/logs/")
    logDir.mkdirs()

    logger.info(s"Mapped ${Utils.formatNumber(mapCount)} records.")
    logger.info(s"Failed to map ${Utils.formatNumber(failureCount)} records.")

    if (failureCount > 0)
      logger.info(s"Error log >> ${logDir.getAbsolutePath}")
    val pw = new PrintWriter(
      new File(s"${logDir.getAbsolutePath}/$shortName-mapping-errors-${System.currentTimeMillis()}.log"))
    errors.foreach(f => pw.write(s"$f\n"))
    pw.close()
  }

  /**
    *
    * @param out
    * @param name
    * @param df
    * @param shortName
    */
  def writeLogsAsCsv(out: String, name: String, df: Dataset[Row], shortName: String): Unit = {
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(out)
  }

  /**
    *
    * @param out
    * @param name
    * @param df
    * @param shortName
    */
  def writeLogsAsTxt(out: String, name: String, df: Dataset[String], shortName: String): Unit = {
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "false")
      .csv(out)
  }

  /**
    * Attempts to reach the Twofishes service
    *
    * @param conf Configuration file
    * @throws RuntimeException If the service cannot be reached
    */
  def pingTwofishes(conf: i3Conf): Unit = {
    val host = conf.twofishes.hostname.getOrElse("localhost")
    val port = conf.twofishes.port.getOrElse("8081")
    val url = s"http://$host:$port/query?query=nyc"

    if (HttpUtils.validateUrl(url)) Unit
    else throw new RuntimeException(s"Cannot reach Twofishes at $url")
  }
}
