package dpla.ingestion3.utils

import java.util

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.services.s3.model.{DeleteObjectsRequest, ListObjectsRequest, ObjectListing}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

trait S3FileHelper {
  lazy val s3client: AmazonS3Client = new AmazonS3Client

  def getBucket(path: String): String = path.split("://")(1).split("/")(0)
  def getKey(path: String): String = path.split("://")(1).split("/").drop(1).mkString("/")

  def deleteS3Path(path: String): Unit = {
    val bucket = getBucket(path)
    val key = getKey(path)

    val listObjectsRequest = new ListObjectsRequest().withBucketName(bucket).withPrefix(key)
    val listObjectsResponse = s3client.listObjects(listObjectsRequest)
    val keys = getS3Keys(listObjectsResponse)

    if (keys.isEmpty)
      return

    deleteS3Keys(bucket, keys)
  }

  def deleteS3Keys(bucket: String, keys: Seq[String]): Unit = {
    val groupedKeys = keys.grouped(1000)
    while(groupedKeys.hasNext) {
      val keyVersions = new util.LinkedList[DeleteObjectsRequest.KeyVersion]
      val group = groupedKeys.next()
      group.map(key => keyVersions.add(new KeyVersion(key)))
      val deleteObjectsRequest = new DeleteObjectsRequest(bucket).withKeys(keyVersions)
      s3client.deleteObjects(deleteObjectsRequest)
    }
  }

  def s3ObjectExists(path: String): Boolean = {
    val bucket = getBucket(path)
    val key = getKey(path)

    val s3client: AmazonS3Client = new AmazonS3Client
    val req = new ListObjectsRequest().withBucketName(bucket).withPrefix(key)
    val rsp = s3client.listObjects(req)
    rsp.getObjectSummaries.size() > 0
  }

  @tailrec
  final def getS3Keys(objects: ObjectListing, files: ListBuffer[String] = new ListBuffer[String]): ListBuffer[String] = {
    files ++= objects.getObjectSummaries.toSeq.map(x => x.getKey)
    if (!objects.isTruncated) files
    else getS3Keys(s3client.listNextBatchOfObjects(objects), files)
  }
}
