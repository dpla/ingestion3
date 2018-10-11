package dpla.ingestion3.dataStorage

import java.io.File

import com.amazonaws.services.s3.model.{ObjectListing, S3ObjectSummary}

import scala.annotation.tailrec
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

object InputHelper {

  /**
    * Check if a given local or S3 path is a valid activity path.
    * An activity is a harvest, mapping, etc.
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
    * @param path Local or S3 path that may or may not be an activity.
    * @return     True if the path matches the pattern for an activity.
    */
  def isActivityPath(path: String): Boolean = {
    val activityPath: Regex = """\d{8}_\d{6}-[A-Za-z]*-[^/]*/?$""".r.unanchored

    path match {
      case activityPath() => true
      case _ => false
    }
  }

  /**
    * Get the most recent activity contained within a given local or S3 folder.
    * The activity must be the immediate child of the given folder.
    *
    * @param path Path to a local or S3 folder.
    * @return     Path to the most recent activity.
    */
  def mostRecent(path: String): Option[String] = {
    Try(parseS3Address(path)) match {
      case Success(address) => mostRecentS3(address)
      case Failure(_) => mostRecentLocal(path)
    }
  }

  /**
    * Sorts the contents of the given path to find the most recent folder.
    * within the provided path that is a valid activity path.
    * The activity must be the immediate child of the given folder.
    *
    * @return Option[String] Absolute path to the most recent activity.
    */
  private def mostRecentLocal(path: String): Option[String] = {
    val rootFile = new File(path)

    rootFile
      .listFiles()
      .filter(f => isActivityPath(f.getName))
      .map(f => f.getAbsolutePath)
      .sorted
      .lastOption
  }

  /**
    * Get the most recent activity from an S3 folder.
    *
    * @param address  The address of the S3 folder.
    * @return         The full path of the most recent activity.
    */
  private def mostRecentS3(address: S3Address): Option[String] = {

    val bucket = address.bucket
    val prefix = address.prefix.getOrElse("")

    // Given that `listObjects' returns results in alphabetical order,
    // and activity folder names begin with a timestamp,
    // we can assume the last item on the last page of results
    // will be from the most recent activity.
    val firstBatch: ObjectListing =  s3client.listObjects(bucket, prefix)
    val lastBatch: ObjectListing = fetchLastS3Batch(firstBatch)
    val objectSummaries: java.util.List[S3ObjectSummary] = lastBatch.getObjectSummaries
    val lastKey = objectSummaries.get(objectSummaries.size - 1).getKey

    // Get the folder directly under the given address.
    val folder: String =
      lastKey.stripPrefix(prefix).stripPrefix("/").split("/")(0)

    val fullPath = S3Address.fullPath(address) + s"/$folder"

    // Ensure that the return value is a valid activity path.
    isActivityPath(fullPath) match {
      case true => Some(fullPath)
      case false => None
    }
  }

  /**
    * Get the last page of results from an S3 object listing.
    *
    * @param ol The first ObjectListing (result of an s3client listObject request)
    * @return   The last ObjectListing
    */
  // TODO: Handle network errors?
  @tailrec
  private def fetchLastS3Batch(ol: ObjectListing): ObjectListing = {
    if (ol.isTruncated) fetchLastS3Batch(s3client.listNextBatchOfObjects(ol))
    else ol
  }
}
