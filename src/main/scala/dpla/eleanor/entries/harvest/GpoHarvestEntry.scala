package dpla.eleanor.entries.harvest

import java.io.{File, FileFilter}
import java.time.Instant

import dpla.eleanor.Schemata
import dpla.eleanor.Schemata.MetadataType
import dpla.eleanor.harvesters.ContentHarvester
import dpla.eleanor.harvesters.providers.GpoHarvester
import dpla.ingestion3.dataStorage.OutputHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}


class XmlFileFilter extends FileFilter {
  override def accept(pathname: File): Boolean = pathname.getName.endsWith("xml")
}

object GpoHarvestEntry {
  def main(args: Array[String]): Unit = {

    // Primary repo for ebooks is s3://dpla-ebooks/
    val rootOutput: String = args.headOption
      .getOrElse(System.getProperty("java.io.tmpdir"))

    val localFiles = args.length match {
      case (2) => new File(args(1))
        .listFiles(new XmlFileFilter)
        .map(_.getAbsoluteFile.toString)
      case _ => Array[String]()
    }

    // Setup output
    val timestamp = new java.sql.Timestamp(Instant.now.getEpochSecond)
    val outputHelper: OutputHelper =
      new OutputHelper(rootOutput, "gpo", "ebook-harvest", timestamp.toLocalDateTime)
    val harvestActivityPath = outputHelper.activityPath

    val conf = new SparkConf()
      .setAppName("Eleanor!")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.broadcastTimeout", "3600")
      .set("spark.driver.memory", "2GB") // increased driver memory
      .setMaster("local[1]") // runs on single executor

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // Harvest metadata
    val metadataHarvester = new GpoHarvester(timestamp, Schemata.SourceUri.Gpo, MetadataType.Rdf)
    val metadataDs = metadataHarvester.execute(spark, localFiles)

    println(s"Harvested metadata records: ${metadataDs.count()}")
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
