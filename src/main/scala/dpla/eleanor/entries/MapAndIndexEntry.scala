package dpla.eleanor.entries

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import dpla.eleanor.Schemata.HarvestData
import dpla.eleanor.Schemata.Implicits.harvestDataEncoder
import dpla.eleanor.{Index, Mapper}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object MapAndIndexEntry {

  def main(args: Array[String]): Unit = {

    // harvest data
    val harvestDataPath: String = args(0)

    // indexing parameters
    val esHost: String = args(1)
    val esPort: String = args(2)
    val esIndexNameBase: String = args(3)
    val shards: Int = if (args.isDefinedAt(4)) args(4).toInt else 5

    // Create indexing timestamp
    val now = Calendar.getInstance().getTime
    val timestamp = getTimestamp(now)
    val esIndexName = s"$esIndexNameBase-$timestamp"

    println(
      f"""
         |Using harvest data $harvestDataPath
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

    val harvest: Dataset[HarvestData] = spark.read
      .parquet(harvestDataPath)
      .as[HarvestData]

    println(s"Read in ${harvest.count()} harvested records from $harvestDataPath")

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
