package dpla.eleanor.entries.harvest

import java.io.File
import java.time.Instant

import dpla.eleanor.Schemata
import dpla.eleanor.Schemata.MetadataType
import dpla.eleanor.harvesters.providers.StandardEbooksHarvester
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

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
    localFiles.foreach(file => println(s"local file > ${file}"))

    val conf = new SparkConf()
      .setAppName("Eleanor!")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.broadcastTimeout", "3600")
      .set("spark.driver.memory", "2GB") // increased driver memory
      .setMaster("local[1]") // runs on single executor

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val timestamp = java.sql.Timestamp.from(Instant.now)

//    println(s"Timestamp: ${timestamp}")

    val harvester = new StandardEbooksHarvester(timestamp, Schemata.SourceUri.StandardEbooks, MetadataType.Opds1)

    val harvest = harvester.execute(
      spark = spark,
      feedUrl = None, // Some("https://standardebooks.org/opds/all"),
      xmlFiles = localFiles,
      out = outPath
    )

    println(s"harvested ${harvest.count}")

    spark.stop()
  }
}
