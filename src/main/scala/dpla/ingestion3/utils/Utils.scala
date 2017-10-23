package dpla.ingestion3.utils

import java.io.File
import java.net.URL
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.xml.Node
import java.util.concurrent.TimeUnit

import org.apache.http.client.fluent.Request
import org.apache.log4j.{FileAppender, LogManager, Logger, PatternLayout}

import scala.util.{Failure, Success, Try}


object Utils {
  /**
    * TODO This should be re-written after an HTTP library is chosen.
    *
    * @param str
    * @return
    */
  def validateUrl(str: String): Boolean = {
    Try { Request.Get(new URL(str).toURI).execute() } match {
      case Success(_) => true
      case Failure(_) => false
    }
  }

  /**
    * Formats the Node in a more human-readable form
    *
    * @param xml An XML node
    * @return Formatted String representation of the node
    */
  def formatXml(xml: Node): String ={
    val prettyPrinter = new scala.xml.PrettyPrinter(80, 2)
    prettyPrinter.format(xml).toString
  }

  /**
    * Print the results of an activity
    *
    * Example:
    *   Record count: 242924 records harvested
    *   Runtime: 4 minutes 24 seconds
    *   Throughput: 920 records/second
    *
    * @param runtime Runtime in milliseconds
    * @param recordCount Number of records output
    */
  def logResults(runtime: Long, recordCount: Long): String = {
    // TODO figure out a better way to share a logger...
    val formatter = java.text.NumberFormat.getIntegerInstance
    val minutes: Long = TimeUnit.MILLISECONDS.toMinutes(runtime)
    val seconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime) -
      TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(runtime))
    val runtimeInSeconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime) + 1
    // add 1 to avoid divide by zero error
    val recordsPerSecond: Long = recordCount/runtimeInSeconds

    s"Harvest complete\n" +
    s"\tRecord count:\t${formatter.format(recordCount)}\n" +
    s"\tRuntime:\t$minutes:$seconds\n" +
    s"\tThroughput:\t${formatter.format(recordsPerSecond)} records/second\n"
  }

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
    * Delete a directory
    * Taken from http://stackoverflow.com/questions/25999255/delete-directory-recursively-in-scala#25999465
    * @param file
    */
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles.foreach(deleteRecursively)
    if (file.exists && !file.delete)
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
  }

  /**
    * Update the query parameters
    *
    * @param listOfParams
    *                     List[ Map[String,String] ]
    *                     A list of Maps to combine
    * @return A single Map
    */
  def updateParams(listOfParams: List[Map[String,String]]): Map[String, String] = {
    listOfParams.flatten.toMap
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
    val date = format.format(Calendar.getInstance().getTime())

    new FileAppender(
      layout,
      s"log/$provider-$process-$date.log",
      true)
  }

  /**
    * Creates a logger object
    *
    * @param operation
    * @param shortName
    * @return
    */
  def createLogger(operation: String, shortName: String = ""): Logger = {
    val logger: Logger = LogManager.getLogger("ingestion3")
    val appender = Utils.getFileAppender(shortName, operation)
    logger.addAppender(appender)
    logger
  }
}
