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
  // Each run gets a unique directory (shortName-millis) so leftover files
  // from a previously killed run — which may be owned by a different OS
  // user — can never block a new harvest from starting.
  private val tmpDir: File = FileUtils.getTempDirectory
  private val tmpDirName: String = s"$shortName-${System.currentTimeMillis()}"
  val tmpOutStr: String = new File(tmpDir, tmpDirName).getAbsolutePath

  // Best-effort cleanup of temp dirs left by prior killed runs.
  // Failures are logged but do not abort startup.
  private val stalePrefix = s"$shortName-"
  Option(tmpDir.listFiles())
    .getOrElse(Array.empty[File])
    .filter(f => f.isDirectory && f.getName.startsWith(stalePrefix) && f.getName != tmpDirName)
    .foreach { dir =>
      Try(Utils.deleteRecursively(dir)).failed.foreach { _ =>
        System.err.println(s"[WARN] LocalHarvester: could not remove stale temp dir: ${dir.getAbsolutePath}")
      }
    }

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

  /** Resolves an endpoint to a local directory. If the endpoint is an S3 URI,
    * syncs or copies its contents to a fresh temp directory and returns that.
    * Otherwise returns the endpoint path as-is.
    *
    * @param endpoint
    *   Local path or s3:// URI
    * @param harvestTime
    *   Timestamp used to make the temp directory name unique
    * @param prefix
    *   Label used to name the temp directory (e.g. "nara-s3")
    * @param awsProfile
    *   Optional AWS named profile for cross-account access
    * @param s3SubCmd
    *   S3 subcommand: "sync" for a directory, "cp" for a single file
    * @return
    *   Local File directory ready for harvesting
    */
  def resolveToLocalDir(
      endpoint: String,
      harvestTime: Long,
      prefix: String,
      awsProfile: Option[String],
      s3SubCmd: String = "sync"
  ): File =
    if (!endpoint.startsWith("s3://")) new File(endpoint)
    else {
      val tmpDir = new File(FileUtils.getTempDirectory, s"$prefix-$harvestTime")
      if (!tmpDir.mkdirs() && !tmpDir.exists())
        throw new RuntimeException(
          s"Failed to create temp directory: ${tmpDir.getAbsolutePath}"
        )
      val profileArgs = awsProfile.toList.flatMap(p => List("--profile", p))
      val dest = if (s3SubCmd == "cp") tmpDir.getAbsolutePath + "/" else tmpDir.getAbsolutePath
      val cmd = List("aws", "s3", s3SubCmd, "--no-progress") ++ profileArgs ++ List(endpoint, dest)
      val proc = new ProcessBuilder(cmd: _*).redirectErrorStream(true).start()
      val output = IOUtils.toString(proc.getInputStream, "UTF-8")
      val exitCode = proc.waitFor()
      if (exitCode != 0)
        throw new RuntimeException(
          s"Failed to $s3SubCmd S3 source (exit $exitCode): $endpoint\n$output"
        )
      tmpDir
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