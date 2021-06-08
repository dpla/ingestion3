package dpla.ingestion3.entries.ingest

import com.databricks.spark.avro.AvroDataFrameReader
import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}
import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.executors.{DplaMap, MappingExecutor}
import dpla.ingestion3.model
import dpla.ingestion3.model.OreAggregation
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.storage.StorageLevel

import java.time.LocalDateTime


object StandaloneMappingEntry extends MappingExecutor {

  def main(args: Array[String]): Unit = {
    val dataIn = "kenning/harvest/20210601_152008-kenning-OriginalRecord.avro"
    val shortName = "kenning"
    val sparkMaster: Option[String] = Some("local[*]")

    val sparkConf = new SparkConf()
      .setAppName(s"Mapping: $shortName")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setMaster("local[*]")

    implicit val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.ui.showConsoleProgress", value = false)
      .getOrCreate()

    val dplaMap = new DplaMap()

    // Load the harvested record dataframe, repartition data
    val harvestedRecords: DataFrame =
      spark
        .read
        .avro(dataIn)
        .repartition(1000)

    for (record <- harvestedRecords.collect()) {

      val document = record.getString(3)
       val mapped = dplaMap.map(document, shortName)
      println(document)

    }

  }
}
