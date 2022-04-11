package dpla.eleanor.entries.harvest

import java.io.File
import java.time.Instant

import dpla.eleanor.Schemata.MetadataType
import dpla.eleanor.Schemata.SourceUri.StandardEbooks
import dpla.eleanor.harvesters.ContentHarvester
import dpla.eleanor.harvesters.providers.StandardEbooksHarvester
import dpla.ingestion3.dataStorage.OutputHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object StandardEbooksHarvestEntry {
  def main(args: Array[String]): Unit = {

    // Primary repo for ebooks is s3://dpla-ebooks/
    val outPath: String = args.headOption
      .getOrElse(System.getProperty("java.io.tmpdir"))

    val localFiles = args.length match {
      case (2) =>
        println(s"Using local files for source ${args(1)}")
        new File(args(1))
        .listFiles()
        .map(_.getAbsoluteFile.toString)
      case _ => Array[String]()
    }

    println(s"Writing harvest output to $outPath")
    localFiles.foreach(file => println(s"local file > $file"))

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
      new OutputHelper(outPath, "standardebooks", "ebook-harvest", timestamp.toLocalDateTime)

    val harvestActivityPath = outputHelper.activityPath

    val metadataHarvester = new StandardEbooksHarvester(timestamp, StandardEbooks, MetadataType.Opds1)
    val metadataDs = metadataHarvester.execute(
      spark = spark,
      feedUrl = None, // Do not harvest from feed, harvest from existing local files
      xmlFiles = localFiles
    )

    println(s"Harvested ${metadataDs.count()}")
    println(s"Writing to $harvestActivityPath")
    metadataDs.write.mode(SaveMode.Overwrite).parquet(harvestActivityPath) // write to activity path

    // TODO add logical check to determine if content should be harvested
    val contentHarvester = new ContentHarvester()
    val contentDs = contentHarvester.harvestContent(metadataDs, spark)
    println(s"Writing content dataset to $harvestActivityPath")
    contentDs.write.mode(SaveMode.Overwrite).parquet(harvestActivityPath) // write to activity path

    spark.stop()
  }
}
