package dpla.ingestion3.utils

import java.io.File

import com.amazonaws.services.s3.model.{ListObjectsRequest, ObjectListing}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}

object MasterDataset {

  def getMostRecentActivities(root: String, providers: Set[String], maxTimestamp: String, activity: String): Seq[String] = {
    if(root.startsWith("s3"))
      buildS3PathList(root, providers, maxTimestamp, activity)
    else
      buildLocalPathList(root, providers, maxTimestamp, activity)
  }

  /**
    * Get a data file path for each provider.
    * The file will be in the specified bucket and directory.
    * The file will be the most recent without surpassing the maxTimestamp.
    *
    * @return           Set[String]   The names of the file paths
    */
  private def buildLocalPathList(root: String, providers: Set[String], maxTimestamp: String, activity: String): Seq[String] = {
    val providerList = getDirs(root)
      .map(_.getName)
      .toSet

    val providersWhitelist =
      if (providers == Set("all")) providerList
      else providers

    val filteredProviderList = providerList.intersect(providersWhitelist)

    val dataPaths = filteredProviderList.flatMap(provider => {
      val providerActivityList = s"$root$provider/$activity/"
      getDirs(providerActivityList)
        .map(_.getAbsolutePath)
        .filter(key => key.compareTo(f"$providerActivityList$maxTimestamp") < 1)
        .sorted
        .lastOption
    })

    dataPaths.toSeq
  }

  private def getDirs(path: String): Seq[File] = new File(path)
      .listFiles()
      .filter(_.isDirectory)

  /**
    * Get a data file path for each provider.
    * The file will be in the specified bucket and directory.
    * The file will be the most recent without surpassing the maxTimestamp.
    *
    * @return           Set[String]   The names of the file paths
    */
  private def buildS3PathList(root: String, providers: Set[String], maxTimestamp: String, activity: String): Seq[String] = {
    // Overcompensating for potential poor S3 bucket definition
    val datasetBucket = root.split("//").map(_.replace("/","")).last

    // List all JSON files
    import scala.collection.JavaConversions._
    val s3: AmazonS3 = AmazonS3ClientBuilder.standard()
      .withRegion("us-east-1")
      .withForceGlobalBucketAccessEnabled(true)
      .build()

    def getPrefixes(bucket: String, prefix: String): Seq[String] = {
      def recurse(objects: ObjectListing, acc: List[String]): Seq[String] = {
        val summaries = objects.getCommonPrefixes.toList
        if (objects.isTruncated) {
          s3.listNextBatchOfObjects(objects)
          recurse(objects, acc ::: summaries)
        } else {
          acc ::: summaries
        }
      }

      val request = new ListObjectsRequest()
        .withBucketName(datasetBucket)
        .withDelimiter("/")
        .withPrefix(prefix)
      val objects: ObjectListing = s3.listObjects(request)
      recurse(objects, List())
    }

    val providerList = getPrefixes(datasetBucket, "").toSet
    val providersWhitelist =
      if (providers == Set("all")) providerList
      else providers.map(p => if (p.endsWith("/")) p else s"$p/")
    val filteredProviderList = providerList.intersect(providersWhitelist)
    val dataPaths = filteredProviderList.flatMap(provider =>
      getPrefixes(datasetBucket, f"$provider$activity/")
        .filter(key => key.compareTo(f"$provider$activity/$maxTimestamp") < 1)
        .sorted
        .lastOption)
      .map(key => f"s3a://$datasetBucket/$key")

    dataPaths.toSeq
  }
}
