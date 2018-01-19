package dpla.ingestion3.utils

import java.io.{File, PrintWriter}
import java.net.URL
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import dpla.ingestion3.confs.i3Conf
import org.apache.log4j.{FileAppender, LogManager, Logger, PatternLayout}
import org.json4s.jackson.JsonMethods._
import org.json4s.JValue

import scala.util.Try
import scala.xml.NodeSeq


object Utils {

  /**
    * Count the number of files in the given directory, outDir.
    *
    * @param outDir Directory to count
    * @param ext File extension to filter by
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
    * @param file File or directory to delete
    */
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  /**
    * Pretty prints JSOn
    *
    * @param data JSON 
    * @return
    */
  def formatJson(data: JValue): String = pretty(render(data))

  /**
    * Format numbers with commas
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
    val minutes: Long = TimeUnit.MILLISECONDS.toMinutes(runtime)
    val seconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime) -
      TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(runtime))
    s"$minutes:$seconds"
  }

  /**
    * Formats the Node in a more human-readable form
    *
    * @param xml An XML node
    * @return Formatted String representation of the node
    */
  def formatXml(xml: NodeSeq): String ={
    val prettyPrinter = new scala.xml.PrettyPrinter(80, 2)
    prettyPrinter.format(xml.head).toString
  }

  /**
    * Uses runtime information to create a log4j file appender.
    *
    * @param provider - Name partner
    * @param process - Process name [harvest, mapping, enrichment]
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
    * Attempts to create a URL object from the string value. If successful
    * returns True, False otherwise
    *
    * @param url String URL
    * @return Boolean
    */
  def isUrl(url: String): Boolean = Try {new URL(url) }.isSuccess


  // TODO These *Summary methods should be refactored and normalized when we fixup logging
  /**
    * Print the results of an activity
    *
    * Example:
    *
    *   Record count: 242,924
    *   Runtime: 4:24
    *   Throughput: 920 records per second
    *
    * @param runtime Runtime in milliseconds
    * @param recordCount Number of records output
    */
  def harvestSummary(runtime: Long, recordCount: Long): String = {
    val recordsPerSecond: Long = recordCount/(runtime/1000)

    s"\n\nRecord count: ${Utils.formatNumber(recordCount)}\n" +
    s"Runtime: ${formatRuntime(runtime)}\n" +
    s"Throughput: ${Utils.formatNumber(recordsPerSecond)} records per second"
  }


  /**
    * Print mapping summary information
    *
    * @param harvestCount Number of harvested records
    * @param mapCount Number of mapped records
    * @param errors Number of mapping failures
    * @param outDir Location to save mapping output
    * @param shortName Provider short name
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
