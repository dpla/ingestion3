package la.dp.ingestion3.indexer

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapred.{FileOutputCommitter, FileOutputFormat, JobConf}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import com.databricks.spark.avro._
import org.apache.spark.rdd.RDD

object IndexerMain {

  val schema: String = """
  | {
  |   "namespace": "la.dp.avro.MAP_3.1",
  |   "type": "record",
  |   "name": "EnrichedRecord",
  |   "doc": "Dumped from PA cqa box in Ingestion 1",
  |   "fields": [
  |     { "name": "id", "type": "string" },
  |     { "name": "json_document", "type": "string" }
  |   ]
  | }
  """.stripMargin

  def main(args:Array[String]): Unit = {

    val input = args(0)
    val cluster = args(1)
    val resource = args(2)

    val conf = new SparkConf().setAppName("CDL Mapping").setMaster("local")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val rawData: RDD[Row] = spark.read
      .format("com.databricks.spark.avro")
      .option("avroSchema", schema)
      .load(input).rdd

    val jobConf = new JobConf(sc.hadoopConfiguration)
    jobConf.setOutputCommitter(classOf[FileOutputCommitter])
    jobConf.set("mapred.output.format.class",  "org.elasticsearch.hadoop.mr.EsOutputFormat")
    jobConf.set(ConfigurationOptions.ES_INPUT_JSON, "yes")
    jobConf.set(ConfigurationOptions.ES_NODES, cluster)
    jobConf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "true")
    jobConf.set(ConfigurationOptions.ES_MAPPING_ID, "_id")
    jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, resource + "/{ingestType}")
    jobConf.set("mapreduce.map.speculative","false")
    jobConf.set("mapreduce.reduce.speculative","false")
    FileOutputFormat.setOutputPath(jobConf, new Path("-"))

    val es: RDD[(String, String)] = rawData.map(
      row => (
        row.getAs[String]("id"),
        row.getAs[String]("json_document")
      )
    )
    es.saveAsHadoopDataset(jobConf)
    FileSystem.closeAll()
    spark.stop()
  }
}
