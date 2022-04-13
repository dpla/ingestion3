package dpla.eleanor.entries

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import dpla.eleanor.Schemata.HarvestData
import dpla.eleanor.Schemata.Implicits.harvestDataEncoder
import dpla.eleanor.{Index, Mapper}
import dpla.ingestion3.utils.MasterDataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object MapAndIndexEntry {

  def main(args: Array[String]): Unit = {

    // ebook-master-dataset path (local or s3)
    val ebookDatasetPath: String = args(0)
    val maxTimestamp: String = if (args.isDefinedAt(1)) args(1) else "now"
    val providers: Set[String] = if (args.isDefinedAt(2)) args(2).split(",").toSet else Set("all")

    // indexing parameters
    val esHost: String = args(3)
    val esPort: String = args(4)
    val esIndexNameBase: String = args(5)
    val shards: Int = if (args.isDefinedAt(6)) args(6).toInt else 6

    // Create indexing timestamp
    val now = Calendar.getInstance().getTime
    val timestamp = getTimestamp(now)
    val esIndexName = s"$esIndexNameBase-$timestamp"

    println(
      f"""
         |Using most recent harvest data in $ebookDatasetPath
         |Creating index $esHost:$esPort/$esIndexName
         |Creating $shards shards
      """.stripMargin)

    val conf = new SparkConf()
      .setAppName("Eleanor!")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    // Roll up most recent ebook-harvest data for each provider
    val mostRecentHarvestPaths = MasterDataset.getMostRecentActivities(ebookDatasetPath, providers, maxTimestamp, "ebook-harvest")

    val harvest: Dataset[HarvestData] = spark.read
      .parquet(mostRecentHarvestPaths:_*)
      .as[HarvestData]

    println(s"Read in ${harvest.count()} harvested records from: \n${mostRecentHarvestPaths.mkString("\n")}")

    val mapped = Mapper.execute(harvest)

    println(s"Mapped ${mapped.count()} records")

    Index.execute(spark, mapped, esHost, esPort, esIndexName, shards)

    println("Indexed mapped records")

    spark.stop()

    println(
      f"""
         |To deploy this index, run the following command
         |
         |
         |curl -XPOST \\
         |  http://$esHost:$esPort/_aliases \\
         |  -H 'Content-Type: application/json' \\
         |  -d '{"actions":[{"remove" : {"index" : "*", "alias" : "dpla_ebooks"}},{"add" : { "index" : "$esIndexName", "alias" : "dpla_ebooks" }}]}'
         |
       """.stripMargin)
  }


  def getTimestamp(now: Date): String = {
    val tsFormat = new SimpleDateFormat("yyyyMMdd-HHmmss")
    tsFormat.format(now)
  }
}
