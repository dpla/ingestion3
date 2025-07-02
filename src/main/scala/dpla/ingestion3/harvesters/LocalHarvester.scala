package dpla.ingestion3.harvesters

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.utils.Utils

import java.io.{File, FileInputStream}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.tools.tar.TarInputStream

import java.util.zip.{GZIPInputStream, ZipInputStream}
import scala.util.Try

/** Abstract class for local harvesters.
 *
 * The harvester abstract class has methods to manage aspects of a harvest that
 * are common among all providers, including:
 *   1. Provide an entry point to run a harvest.
 *   2. If the output directory  already exists, delete its contents.
 *   3. Log and timestamp the beginning and end of a harvest.
 *   4. Log information about the completed harvest.
 *   5. Validate the schema final DataFrame (only for logging purposes).
 *
 * @param shortName
 *   [String] Provider short name
 * @param conf
 *   [i3Conf] contains configs for the harvester.
 */
abstract class LocalHarvester(
                               shortName: String,
                               conf: i3Conf,
                             ) extends Harvester {

  // Temporary output path.
  // Harvests that use AvroWriter cannot be written directly to S3.
  // Instead, they are written to this temp path,
  //   then loaded into a spark DataFrame,
  //   then written to their final destination.
  // TODO: make tmp path configurable rather than hard-coded
  val tmpOutStr: String = new File(FileUtils.getTempDirectory, shortName).getAbsolutePath

  // Delete temporary output directory and files if they already exist.
  Utils.deleteRecursively(new File(tmpOutStr))

  private val avroWriter: DataFileWriter[GenericRecord] =
    AvroHelper.avroWriter(shortName, tmpOutStr, Harvester.schema)

  def writeOut(unixEpoch: Long, item: ParsedResult): Unit = {
    val genericRecord = new GenericData.Record(Harvester.schema)
    genericRecord.put("id", item.id)
    genericRecord.put("ingestDate", unixEpoch)
    genericRecord.put("provider", shortName)
    genericRecord.put("document", item.item)
    genericRecord.put("mimetype", mimeType)
    avroWriter.append(genericRecord)
  }

  override def close(): Unit = {
    avroWriter.flush()
    avroWriter.close()
  }

  override def cleanUp(): Unit =
    Utils.deleteRecursively(new File(tmpOutStr))

}

case class FileResult(
                       entryName: String,
                       data: Option[Array[Byte]]
                     )

case class ParsedResult(id: String, item: String)

object LocalHarvester {

  /** Loads .tar.gz files
   *
   * @param file
   *   File to parse
   * @return
   *   Option[TarInputStream] of the zip contents
   */
  def getTarInputStream(file: File): Option[TarInputStream] = {
    file.getName match {
      case zipName if zipName.endsWith("tar.gz") || zipName.endsWith(".tgz") =>
        Some(new TarInputStream(new GZIPInputStream(new FileInputStream(file))))
      case zipName if zipName.endsWith("tar") =>
        Some(new TarInputStream(new FileInputStream(file)))
      case _ => None
    }
  }

  def getZipInputStream(file: File): Option[ZipInputStream] =
    file.getName match {
      case zipName if zipName.endsWith("zip") =>
        Some(new ZipInputStream(new FileInputStream(file)))
      case _ => None
    }

  private def shouldSkipEntry(entryName: String, isDirectory: Boolean): Boolean =
    isDirectory || entryName.contains("._") // skip MacOS metadata files

  def iter(zipInputStream: ZipInputStream): LazyList[FileResult] =
    Option(zipInputStream.getNextEntry) match {
      case None =>
        LazyList.empty
      case Some(entry) =>
        val result =
          if (shouldSkipEntry(entry.getName, entry.isDirectory))
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
            shouldSkipEntry(filename, entry.isDirectory)
          )
            None
          else if (filename.endsWith(".xml")) // only read xml files
            Some(IOUtils.toByteArray(tarInputStream, entry.getSize))
          else
            None

        FileResult(entry.getName, result) #:: iter(tarInputStream)
    }
}