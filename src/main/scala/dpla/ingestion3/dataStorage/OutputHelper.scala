package dpla.ingestion3.dataStorage

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import dpla.ingestion3.utils.FlatFileIO

import java.io.ByteArrayInputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.amazonaws.services.s3.model.{ObjectMetadata, PutObjectRequest}

import scala.util.{Failure, Success, Try}

/** @param root:
  *   Root directory or AWS S3 bucket. If AWS S3 bucket, should start with
  *   "s3a://".
  * @param shortName:
  *   Provider short name
  * @param activity:
  *   "harvest", "mapping", "enrichment", etc.
  * @param startDateTime:
  *   Start dateTime of the activity
  *
  * @throws IllegalArgumentException
  *
  * @see
  *   https://digitalpubliclibraryofamerica.atlassian.net/wiki/spaces/TECH/pages/84512319/Ingestion+3+Storage+Specification
  *   for details on file naming conventions
  */
class OutputHelper(
    root: String,
    shortName: String,
    activity: String,
    startDateTime: LocalDateTime
) {

  private val s3WriteProtocol: String = "s3a"

  private val s3Client = AmazonS3ClientBuilder.defaultClient()

  /** If the given root is an S3 path, parse an S3Address. Evaluate on
    * instantiation so invalid S3 protocol is caught immediately. This val can
    * be used to check wither a given root is an S3 path or not.
    *
    * @throws IllegalArgumentException
    *   if given root is S3 path with invalid protocol for writing files
    */
  val s3Address: Option[S3Address] = Try(parseS3Address(root)) match {
    // root is a valid S3 path
    case Success(a) => {
      if (a.protocol != s3WriteProtocol)
        // TODO: Instead of throwing exception, override given protocol and use s3a?
        throw new IllegalArgumentException(
          s"$s3WriteProtocol protocol required for writing output"
        )
      Some(a)
    }
    // root is not a valid S3 path
    case Failure(_) => None
  }

  // Evaluate on instantiation so that invalid `activity' is caught immediately.
  // TODO: make schema configurable - could use sealed case classes for activities
  private val schema: String = activity match {
    case "harvest"       => "OriginalRecord.avro"
    case "mapping"       => "MAP4_0.MAPRecord.avro"
    case "enrichment"    => "MAP4_0.EnrichRecord.avro"
    case "jsonl"         => "MAP3_1.IndexRecord.jsonl"
    case "reports"       => "reports"
    case "topic_model"   => "topic_model.parquet"
    case "wiki"          => "wiki.parquet"
    case "ebook-harvest" => "OriginalRecord.parquet"
    case _ =>
      throw new IllegalArgumentException(s"Activity '$activity' not recognized")
  }

  // TODO: Remove this dependency on utils?  Move FileIO to dataStorage or re-implement method?
  private lazy val flatFileIO = new FlatFileIO

  private lazy val timestamp: String =
    startDateTime.format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))

  /** S3 bucket or root directory for output. Includes trailing slash. If S3
    * bucket, includes "s3a://" prefix.
    */
  private lazy val rootPath: String =
    if (root.endsWith("/")) root else s"$root/"

  private val activityRelativePath: String =
    s"$shortName/$activity/$timestamp-$shortName-$schema"

  private lazy val manifestRelativePath: String =
    s"$activityRelativePath/_MANIFEST"

  private lazy val setSummaryRelativePath: String =
    s"$activityRelativePath/_OAI_SET_SUMMARY"

  private lazy val summaryRelativePath: String =
    s"$activityRelativePath/_SUMMARY"

  private lazy val logsRelativePath: String =
    s"$activityRelativePath/_LOGS"

  lazy val activityPath = s"$rootPath$activityRelativePath"

  lazy val manifestPath = s"$rootPath$manifestRelativePath"

  lazy val setSummaryPath = s"$rootPath$setSummaryRelativePath"

  lazy val summaryPath = s"$rootPath$summaryRelativePath"

  lazy val logsPath = s"$rootPath$logsRelativePath"

  /** Write a manifest file in the given outputPath directory.
    *
    * @param opts:
    *   Optional data points to be included in the manifest file.
    *
    * @return
    *   Try[String]: Path of output file.
    */
  def writeManifest(opts: Map[String, String]): Try[String] = {

    val text: String = manifestText(opts)

    s3Address match {
      case Some(a) => {
        val bucket = a.bucket
        val key = Array(a.prefix, Some(manifestRelativePath)).flatten
          .mkString("/")
        writeS3File(bucket, key, text)
      }
      case None => writeLocalFile(manifestPath, text)
    }
  }

  /** Write a group by set and record count of OAI sets
    *
    * @param summary
    *   String of group by set and record count
    * @return
    *   Try[String]: Path of the output file
    */
  def writeSetSummary(summary: String): Try[String] = {
    s3Address match {
      case Some(a) => {
        val bucket = a.bucket
        val key = Array(a.prefix, Some(setSummaryRelativePath)).flatten
          .mkString("/")
        writeS3File(bucket, key, summary)
      }
      case None => writeLocalFile(setSummaryPath, summary)
    }
  }

  /** Write a summary file in the given outputPath directory.
    *
    * @param summary
    *   Summary text blob
    * @return
    *   Try[String]: Path of the output file
    */
  def writeSummary(summary: String): Try[String] = {
    s3Address match {
      case Some(a) => {
        val bucket = a.bucket
        val key = Array(a.prefix, Some(summaryRelativePath)).flatten
          .mkString("/")
        writeS3File(bucket, key, summary)
      }
      case None => writeLocalFile(summaryPath, summary)
    }
  }

  /** Create text for a manifest file.
    *
    * opts: Optional data points to be included in the manifest file. This is
    * intentionally open-ended so that individual executors can include whatever
    * data points are relevant to their activity.
    *
    * @return
    *   Text string.
    */
  private val manifestText: Map[String, String] => String =
    (opts: Map[String, String]) => {

      val date: String = startDateTime
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

      // Add date/time to given `opts'
      val data: Map[String, String] = opts + ("Start date/time" -> date)

      data.map { case (k, v) => s"$k: $v" }.mkString("\n")
    }

  // TODO: Move file writers?

  /** Write a String to a local file.
    *
    * @param outPath:
    *   Output path
    * @param text:
    *   Text string to be written to local file
    *
    * @return
    *   Try[String]: Path of output file.
    */
  def writeLocalFile(outPath: String, text: String): Try[String] =
    Try { flatFileIO.writeFile(text, outPath) }

  /** Write a String to an S3 file.
    *
    * @param bucket:
    *   S3 bucket (do not include trailing slash or "s3a://" prefix)
    * @param key:
    *   S3 file key
    * @param text:
    *   Text string to be written to S3 file
    *
    * @return
    *   Try[String] Path of written file.
    */
  def writeS3File(bucket: String, key: String, text: String): Try[String] =
    Try {
      val in = new ByteArrayInputStream(text.getBytes("utf-8"))
      s3Client.putObject(
        new PutObjectRequest(bucket, key, in, new ObjectMetadata)
      )
      // Return filepath
      s"$bucket/$key"
    }
}
