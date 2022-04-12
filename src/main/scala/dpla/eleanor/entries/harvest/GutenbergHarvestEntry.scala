package dpla.eleanor.entries.harvest

import java.io.{File, FileFilter}
import java.time.Instant

import dpla.eleanor.Schemata
import dpla.eleanor.Schemata.MetadataType
import dpla.eleanor.harvesters.ContentHarvester
import dpla.eleanor.harvesters.providers.GutenbergHarvester
import dpla.ingestion3.dataStorage.OutputHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

class ZipFileFilter extends FileFilter with Serializable {
  override def accept(pathname: File): Boolean = pathname.getName.endsWith("zip")
}

/**
  *
  *
  * Example Invocations:
  *
  * Write to local storage, do not harvest content (e.g. files), harvest metadata from local files
  *   sbt "runMain dpla.eleanor.entries.harvest.GutenbergHarvestEntry ./eleanor-dataset/ false ./eleanor/raw-data/gpo/"
  *
  * Write to S3, harvest content (e.g. files) and harvest metadata from metadata feed
  *   sbt "runMain dpla.eleanor.entries.harvest.GutenbergHarvestEntry s3://dpla-ebooks-master-dataset/ true"
  *
  * Arguments:
  *
  *   0) rootOutput                 Root output path, local or S3. Defaults to local system /tmp/
  *   1) performContentHarvest      Boolean, whether to execute content harvest along with metadata harvest. Defaults to
  *                                 to True
  *   2) localFiles                 Path to local metadata files to harvest
  */
object GutenbergHarvestEntry {
  /**
    * Responsible for executing the Gutenberg harvest
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    // Primary repo for ebooks is s3://dpla-ebooks/
    val rootOutput: String = if (args.isDefinedAt(0)) args(0) else System.getProperty("java.io.tmpdir")
    val performContentHarvest: Boolean = if(args.isDefinedAt(1)) args(1).toBoolean else true
    val localFiles: Array[String] = if (args.isDefinedAt(2)) {
      new File(args(2)).listFiles(new ZipFileFilter).map(_.getAbsoluteFile.toString)
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
    val timestamp = new java.sql.Timestamp(Instant.now.getEpochSecond)
    val outputHelper: OutputHelper =
      new OutputHelper(rootOutput, "gutenberg", "ebook-harvest", timestamp.toLocalDateTime)
    val harvestActivityPath = outputHelper.activityPath

    // Harvest metadata
    println(s"Harvesting metadata to $harvestActivityPath")
    val metadataHarvester = new GutenbergHarvester(timestamp, Schemata.SourceUri.Gutenberg, MetadataType.Rdf)
    val metadataDs = metadataHarvester.execute(spark = spark, files = localFiles)
    metadataDs.write.mode(SaveMode.Overwrite).parquet(harvestActivityPath)

    if(performContentHarvest) {
      println("Harvesting content")
      val contentHarvester = new ContentHarvester()
      val contentDs = contentHarvester.harvestContent(metadataDs, spark)
      println(s"Writing to $harvestActivityPath")
      contentDs.write.mode(SaveMode.Overwrite).parquet(harvestActivityPath)
    }

    spark.stop()
  }
}
