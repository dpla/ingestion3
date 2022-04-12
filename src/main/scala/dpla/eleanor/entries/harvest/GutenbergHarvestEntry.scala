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

object GutenbergHarvestEntry {
  def main(args: Array[String]): Unit = {

    // Primary repo for ebooks is s3://dpla-ebooks/
    val rootOutput: String = args.headOption
      .getOrElse(System.getProperty("java.io.tmpdir"))

    val localFiles = args.length match {
      case (2) => new File(args(1))
        .listFiles(new ZipFileFilter)
        .map(_.getAbsoluteFile.toString)
      case _ => Array[String]()
    }
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

    // Harvest content
    val contentHarvester = new ContentHarvester()
    // Run over HarvestData Dataset and download content/payloads
    val contentDs = contentHarvester.harvestContent(metadataDs, spark)
    println(s"Writing to $harvestActivityPath")
    contentDs.write.mode(SaveMode.Overwrite).parquet(harvestActivityPath)

    println(s"harvested ${contentDs.count}")

    spark.stop()
  }
}
