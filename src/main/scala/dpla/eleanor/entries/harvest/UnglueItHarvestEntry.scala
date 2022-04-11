package dpla.eleanor.entries.harvest

import java.io.File
import java.time.Instant

import dpla.eleanor.Schemata.MetadataType
import dpla.eleanor.Schemata.SourceUri.UnglueIt
import dpla.eleanor.harvesters.opds1.Opds1Harvester
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object UnglueItHarvestEntry {
  def main(args: Array[String]): Unit = {

    // Primary repo for ebooks is s3://dpla-ebooks/
    val outPath: String = args.headOption
      .getOrElse(System.getProperty("java.io.tmpdir"))

    val localFiles = args.length match {
      case (2) => new File(args(1))
        .listFiles()
        .map(_.getAbsoluteFile.toString)
      case _ => Array[String]()
    }

    println(s"Writing harvest output to $outPath")

    val conf = new SparkConf()
      .setAppName("Eleanor!")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.broadcastTimeout", "3600")
      .set("spark.driver.memory", "2GB") // increased driver memory
      .setMaster("local[1]") // runs on single executor

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val timestamp = new java.sql.Timestamp(Instant.now.getEpochSecond)
    val harvester = new Opds1Harvester(timestamp, UnglueIt, "unglueit", MetadataType.Opds1)

    val harvest = harvester.execute(
      spark = spark,
      feedUrl = None, // Do not harvest from feed, harvest from existing local files
      xmlFiles = localFiles,
      rootOutput = outPath
    )

    println(s"harvested ${harvest.count}")

    spark.stop()
  }
}
