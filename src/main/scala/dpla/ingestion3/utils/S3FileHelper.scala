package dpla.ingestion3.utils

import java.util
import com.amazonaws.services.s3.{
  AmazonS3,
  AmazonS3Client,
  AmazonS3ClientBuilder
}
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion
import com.amazonaws.services.s3.model.{
  DeleteObjectsRequest,
  ListObjectsRequest,
  ObjectListing
}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

trait S3FileHelper {

  private lazy val s3client: AmazonS3 = AmazonS3ClientBuilder.defaultClient()

  def getKey(path: String): String =
    path.split("://")(1).split("/").drop(1).mkString("/")

  def deleteS3Keys(bucket: String, keys: Seq[String]): Unit = {
    val groupedKeys = keys.grouped(1000)
    while (groupedKeys.hasNext) {
      val keyVersions = new util.LinkedList[DeleteObjectsRequest.KeyVersion]
      val group = groupedKeys.next()
      group.map(key => keyVersions.add(new KeyVersion(key)))
      val deleteObjectsRequest = new DeleteObjectsRequest(bucket).withKeys(keyVersions)
      s3client.deleteObjects(deleteObjectsRequest)
    }
  }
}
