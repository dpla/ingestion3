package dpla.ingestion3.utils

import java.io.File
import java.net.URL

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io.Text

import scala.xml.Node
import java.util.concurrent.TimeUnit

import org.apache.commons.codec.digest.DigestUtils
import org.apache.http.client.fluent.Request

import scala.util.{Failure, Success, Try}

/**
  * Created by scott on 1/18/17.
  */
object Utils {
  /**
    * Accepts a String URL and validates it by making a request and returning true only if
    * the response code is a 200.
    *
    * @param str
    * @return
    */
  def validateUrl(str: String): Boolean = {
    def url() : Try[URL] = Try {
      new URL(str) // with throw exception if string is not valid URL
    }

    url() match {
      case Success(endpoint) => {
        val code = Request.Get(endpoint.toURI)
          .execute()
          .returnResponse()
          .getStatusLine
          .getStatusCode
        // TODO Other appropriate codes or something like (code / 100) match case 2 => ?
        code match {
          case 200 => true
          case _ => false
        }
      }
      case Failure(_) => false
    }
  }

  /**
    * Create an md5 hash from the given id value
    *
    * @param id Value to hash
    * @return MD5 hash
    * @throws IllegalArgumentException If no source value given
    */
  def generateMd5(id: Option[String]): String = {
    id match {
      case Some(i) => DigestUtils.md5Hex(i)
      // TODO: Is more information required in this error message?
      case _ => throw new IllegalArgumentException("Unable to mint an MD5 ID for record.")
    }
  }

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
    *   Harvest count: 242924 records harvested
    *   Runtime: 4 minutes 24 seconds
    *   Throughput: 920 records/second
    *
    * @param runtime Runtime in milliseconds
    * @param recordCount Number of records output
    */
  def printResults(runtime: Long, recordCount: Long): Unit = {
    val formatter = java.text.NumberFormat.getIntegerInstance
    val minutes: Long = TimeUnit.MILLISECONDS.toMinutes(runtime)
    val seconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime) -
      TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(runtime))
    val runtimeInSeconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime) + 1
    // add 1 to avoid divide by zero error
    val recordsPerSecond: Long = recordCount/runtimeInSeconds

    println(s"File count: ${formatter.format(recordCount)}")
    println(s"Runtime: $minutes:$seconds")
    println(s"Throughput: ${formatter.format(recordsPerSecond)} records/second")
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
    * Print the results of a harvest
    *
    * Example:
    *   Harvest count: 242924 records harvested
    *   Runtime: 4 minutes 24 seconds
    *   Throughput: 920 records/second
    *
    * @param runtime Runtime in milliseconds
    * @param recordsHarvestedCount Number of records in the output directory
    */

  def printRuntimeResults(runtime: Long, recordsHarvestedCount: Long): Unit = {
    // Make things pretty
    val formatter = java.text.NumberFormat.getIntegerInstance
    val minutes: Long = TimeUnit.MILLISECONDS.toMinutes(runtime)
    val seconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime) -
      TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(runtime))
    val runtimeInSeconds: Long = TimeUnit.MILLISECONDS.toSeconds(runtime) + 1
    // add 1 to avoid divide by zero error
    val recordsPerSecond: Long = recordsHarvestedCount/runtimeInSeconds

    println(s"\n\nFile count: ${formatter.format(recordsHarvestedCount)}")
    println(s"Runtime: $minutes:$seconds")
    println(s"Throughput: ${formatter.format(recordsPerSecond)} records/second")
  }
}
