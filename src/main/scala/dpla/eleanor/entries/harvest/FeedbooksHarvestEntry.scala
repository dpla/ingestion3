package dpla.eleanor.entries.harvest

import java.io.File
import java.time.{Instant, LocalDateTime}

import dpla.eleanor.Schemata
import dpla.eleanor.Schemata.MetadataType
import dpla.eleanor.harvesters.ContentHarvester
import dpla.eleanor.harvesters.opds1.Opds1Harvester
import dpla.ingestion3.dataStorage.OutputHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  *
  *
  * Example Invocations:
  *
  * Write to local storage, do not harvest content (e.g. files), harvest metadata from local files
  *   sbt "runMain dpla.eleanor.entries.harvest.GpoHarvestEntry /Users/scott/dpla/eleanor/ false /Users/scott/dpla/code/eleanor/raw-data/gpo/"
  *
  * Write to S3, harvest content (e.g. files) and harvest metadata from metadata feed
  *   sbt "runMain dpla.eleanor.entries.harvest.GpoHarvestEntry s3://dpla-ebooks-master-dataset/ true"
  *
  * Arguments:
  *
  *   0) rootPath                   Root output path, local or S3. Defaults to local system /tmp/
  *   1) performContentHarvest      Boolean, whether to execute content harvest along with metadata harvest. Defaults to
  *                                 to True
  *   2) localFiles                 Path to local metadata files to harvest
  */

object FeedbooksHarvestEntry {
  /**
    * Responsible for executing the Feedbooks harvest
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // Primary repo for ebooks is s3://dpla-ebooks/
    val rootPath: String = if (args.isDefinedAt(0)) args(0) else System.getProperty("java.io.tmpdir")
    val performContentHarvest: Boolean = if(args.isDefinedAt(1)) args(1).toBoolean else true
    val localFiles: Array[String] = if (args.isDefinedAt(2)) {
      new File(args(2)).listFiles.map(_.getAbsoluteFile.toString)
    } else Array[String]()

    val conf = new SparkConf()
      .setAppName("Eleanor!")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.broadcastTimeout", "3600")
      .set("spark.driver.memory", "2GB") // increased driver memory
      .setMaster("local[1]") // runs on single executor

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // Sets the activity output path with timestamp
    val startDateTime = LocalDateTime.now
    val outputHelper: OutputHelper =
      new OutputHelper(rootPath, "feedbooks", "ebook-harvest", startDateTime)
    val harvestActivityPath = outputHelper.activityPath

    val timestamp = new java.sql.Timestamp(Instant.now.getEpochSecond)
    val metadataHarvester = new Opds1Harvester(timestamp, Schemata.SourceUri.Feedbooks, MetadataType.Opds1)
    val metadataDs = metadataHarvester.execute(
      spark = spark,
      feedUrl = None, // Do not harvest from feed, harvest from existing local files
      xmlFiles = localFiles
    )

    println(s"Harvested ${metadataDs.count()}")
    println(s"Writing to $harvestActivityPath")
    metadataDs.write.mode(SaveMode.Overwrite).parquet(harvestActivityPath) // write to activity path

    if(performContentHarvest) {
      println(s"Harvesting content")
      val contentHarvester = new ContentHarvester()
      val contentDs = contentHarvester.harvestContent(metadataDs, spark)
      println(s"Writing content dataset to $harvestActivityPath")
      contentDs.write.mode(SaveMode.Overwrite).parquet(harvestActivityPath) // write to activity path
    }

    spark.stop()
  }
}
