package dpla.eleanor.entries.harvest

import java.io.{File, FileFilter}
import java.time.Instant

import dpla.eleanor.Schemata
import dpla.eleanor.Schemata.MetadataType
import dpla.eleanor.harvesters.providers.GutenbergHarvester
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class ZipFileFilter extends FileFilter {
  override def accept(pathname: File): Boolean = pathname.getName.endsWith("zip")
}

object GutenbergHarvestEntry {
  def main(args: Array[String]): Unit = {

    // Primary repo for ebooks is s3://dpla-ebooks/
    val outPath: String = args.headOption
      .getOrElse(System.getProperty("java.io.tmpdir"))

    val localFiles = args.length match {
      case (2) => new File(args(1))
        .listFiles(new ZipFileFilter)
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

    val harvester = new GutenbergHarvester(timestamp, Schemata.SourceUri.Gutenberg, MetadataType.Rdf)

    val harvest = harvester.execute(
      spark = spark,
      files = localFiles,
      out = outPath
    )

    println(s"harvested ${harvest.count}")

    spark.stop()
  }
}
