package dpla.ingestion3.utils

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.log4j.{FileAppender, LogManager, Logger, PatternLayout}

import scala.xml.Node


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
  def formatXml(xml: Node): String ={
    val prettyPrinter = new scala.xml.PrettyPrinter(80, 2)
    prettyPrinter.format(xml).toString
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
  def summarizeResults(runtime: Long, recordCount: Long): String = {
    val recordsPerSecond: Long = recordCount/(runtime/1000)
    val formatter = java.text.NumberFormat.getIntegerInstance

    s"\n\nRecord count: ${formatter.format(recordCount)}\n" +
    s"Runtime: ${formatRuntime(runtime)}\n" +
    s"Throughput: ${formatter.format(recordsPerSecond)} records per second"
  }
}
