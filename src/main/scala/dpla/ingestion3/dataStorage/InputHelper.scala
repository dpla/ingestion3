package dpla.ingestion3.dataStorage

import java.io.File

import com.amazonaws.services.s3.model.{ObjectListing, S3ObjectSummary}

import scala.annotation.tailrec
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object InputHelper {

  /**
    * Check if a given path is a valid activity path.
    *
    * A valid path will match the following pattern:
    *   8 digits (year month day)
    *   underscore
    *   6 digits (time)
    *   dash
    *   0-n letters (provider short name)
    *   dash
    *   0-n characters (not "/") (schema)
    *   0-1 "/"
    *
    * @param path
    * @return Boolean
    */
  def isActivityPath(path: String): Boolean = {
    val activityPath: Regex = """\d{8}_\d{6}-[A-Za-z]*-[^/]*/?$""".r.unanchored

    path match {
      case activityPath() => true
      case _ => false
    }
  }

  def mostRecent(path: String): Option[String] = {
    Try(parseS3Address(path)) match {
      case Success(address) => mostRecentS3(address)
      case Failure(_) => mostRecentLocal(path)
    }
  }

  /**
    * Sorts the contents of the given path to find the most recent folder
    * within the provided path that ends with '.avro'
    *
    * @return Option[String] Absolute path to the most recent data within folder
    *
    */
  private def mostRecentLocal(path: String): Option[String] = {
    val rootFile = new File(path)

    rootFile
      .listFiles()
      .filter(f => f.getName.endsWith(".avro"))
      .map(f => f.getAbsolutePath)
      .sorted
      .lastOption
  }

  /**
    * This assumes that the given address only contains properly formatted
    * activity files.
    *
    * @param address
    * @return
    */
  private def mostRecentS3(address: S3Address): Option[String] = {

    val bucket = address.bucket
    val prefix = address.prefix.getOrElse("")

    // Given that `listObjects' returns results in alphabetical order,
    // and files are timestamped,
    // we can assume the last item on the last page of results
    // will be from the most recent activity.
    val firstBatch: ObjectListing =  s3client.listObjects(bucket, prefix)
    val lastBatch: ObjectListing = fetchLastS3Batch(firstBatch)
    val objectSummaries: java.util.List[S3ObjectSummary] = lastBatch.getObjectSummaries
    val lastKey = objectSummaries.get(objectSummaries.size - 1).getKey

    // Get the folder directly under the given address.
    val folder: Option[String] =
      lastKey.stripPrefix(prefix).stripPrefix("/").split("/").headOption

    // Return the full S3 path.
    folder match {
      case Some(f) => Some(S3Address.fullPath(address) + s"/$f")
      case None => None
    }
  }

  // TODO: Handle network errors?
  @tailrec
  private def fetchLastS3Batch(ol: ObjectListing): ObjectListing = {
    if (ol.isTruncated) fetchLastS3Batch(s3client.listNextBatchOfObjects(ol))
    else ol
  }
}
