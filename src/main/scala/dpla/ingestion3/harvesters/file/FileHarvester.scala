package dpla.ingestion3.harvesters.file

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.LocalHarvester

import java.io.{File, FileInputStream}
import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SparkSession
import org.apache.tools.tar.TarInputStream

import java.util.zip.{GZIPInputStream, ZipInputStream}
import scala.util.Try

/** File based harvester
  *
  * @param shortName
  *   Provider short name
  * @param conf
  *   Configurations
  * @param logger
  *   Logger
  */
abstract class FileHarvester(
    spark: SparkSession,
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(spark, shortName, conf) {

  /** Parses and extracts ZipInputStream and writes parses records out.
    *
    * @param fileResult
    *   Case class representing extracted items from a compressed file
    * @return
    *   Count of metadata items found.
    */
  def handleFile(fileResult: FileResult, unixEpoch: Long): Try[Int]

  /** Writes item out
    *
    * @param unixEpoch
    *   Timestamp of the harvest
    * @param item
    *   Harvested record
    */


  def flush(): Unit = getAvroWriter.flush()


}

/** Case class to hold the results of a file
 *
 * @param entryName
 *   Path of the entry in the file
 * @param data
 *   Holds the data for the entry, or None if it's a directory.
 * @param bufferedData
 *   Holds a buffered reader for the entry if it's too large to be held in
 *   memory.
 */

case class FileResult(
                       entryName: String,
                       data: Option[Array[Byte]]
                     )

/** Case class hold the parsed value from a given FileResult
 */

case class ParsedResult(id: String, item: String)

object FileHarvester {

  def getZipInputStream(file: File): Option[ZipInputStream] =
    file.getName match {
      case zipName if zipName.endsWith("zip") =>
        Some(new ZipInputStream(new FileInputStream(file)))
      case _ => None
    }

  /** Loads .tar.gz files
   *
   * @param file
   *   File to parse
   * @return
   *   Option[TarInputStream] of the zip contents
   */
  def getTarInputStream(file: File): Option[TarInputStream] = {
    file.getName match {
      case zipName if zipName.endsWith("gz") =>
        Some(new TarInputStream(new GZIPInputStream(new FileInputStream(file))))
      case zipName if zipName.endsWith("tar") =>
        Some(new TarInputStream(new FileInputStream(file)))
      case _ => None
    }
  }


  def iter(zipInputStream: ZipInputStream): LazyList[FileResult] =
    Option(zipInputStream.getNextEntry) match {
      case None =>
        LazyList.empty
      case Some(entry) =>
        val result =
          if (entry.isDirectory || entry.getName.contains("._"))
            None
          else
            Some(IOUtils.toByteArray(zipInputStream))
        FileResult(entry.getName, result) #:: iter(zipInputStream)
    }

  def iter(tarInputStream: TarInputStream): LazyList[FileResult] =
    Option(tarInputStream.getNextEntry) match {
      case None =>
        LazyList.empty

      case Some(entry) =>
        val filename = Try {
          entry.getName
        }.getOrElse("")

        val result =
          if (
            entry.isDirectory || filename.contains("._")
          ) // drop OSX hidden files
            None
          else if (filename.endsWith(".xml")) // only read xml files
            Some(IOUtils.toByteArray(tarInputStream, entry.getSize))
          else
            None

        FileResult(entry.getName, result) #:: iter(tarInputStream)
    }
}
