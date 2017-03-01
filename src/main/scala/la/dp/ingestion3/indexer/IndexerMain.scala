package la.dp.ingestion3.indexer

import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, SparkSession}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions
import org.elasticsearch.hadoop.mr.EsOutputFormat.EsOldAPIOutputCommitter

/**
  * This class implements a Spark pipeline for indexing Avro files of JSON documents that are formatted in DPLA MAP v3.1
  * The indexer is capable of handling documents of multiple types existing in the same Avro collection.
  * There are a number of hardcoded configuration options set on the JobConf object that are essentially there to get
  * the legacy ElasticSearch Hadoop code to work with our data. However, some generalization is possible. In the future,
  * when DPLA migrates to more modern search infrastructure, this code should be rewritten to use the Spark native
  * Elasticsearch implementation, which will be cleaner and eliminate the code that calls Hadoop APIs.
  *
  * Expected input: a directory containing Avro files full of JSON documents that match the intended Elasticsearch index
  * schema.
  *
  * Expected output: Elasticsearch REST API calls to populate the specified index, but nothing filesystem-related.
  */

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
  """.stripMargin //todo retrieve this from S3

  def main(args:Array[String]): Unit = {

    val input = args(0)
    val cluster = args(1)
    val index = args(2)

    val conf = new SparkConf()
      .setAppName("Ingest 3 Indexer")
      //todo this should be a parameter
      .setMaster("local")
      //This enables object serialization using the Kryo library
      //rather than Java's builtin serialization. It's compatible
      //with more classes and doesn't require that the class to be
      //serialized implements the Serializable interface.
      .set("spark.serializer", classOf[KryoSerializer].getName)

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    //Using this API for reading Avro means that we get the
    val rawData: RDD[Row] = spark.read
      .format("com.databricks.spark.avro")
      .option("avroSchema", schema)
      .load(input).rdd

    //The following configuration stuff is basically necessary because we're using an old version of elasticsearch.
    //This means that we need to use the older elasticsearch-hadoop tooling, which was built for Hadoop.
    //Spark is able to take advantage of it with the saveAsHadoopDataset call below, but later versions of ES support
    //a Spark-native approach that is a lot easier to use and would eliminate Hadoop APIs and dependencies.
    val jobConf = new JobConf(sc.hadoopConfiguration)
    //This class tells Hadoop how to finish off saving the output data. This is a no-op implementation.
    jobConf.setOutputCommitter(classOf[EsOldAPIOutputCommitter])
    //This tells the Hadoop API how to save the resulting documents, i.e., to ElasticSearch
    jobConf.set("mapred.output.format.class",  "org.elasticsearch.hadoop.mr.EsOutputFormat")
    //This means we're giving it json instead of ES API record objects:
    jobConf.set(ConfigurationOptions.ES_INPUT_JSON, "yes")
    //This passes info about the cluster we're writing to
    jobConf.set(ConfigurationOptions.ES_NODES, cluster)
    //This tells it to create the index if it doesn't exist.
    jobConf.set(ConfigurationOptions.ES_INDEX_AUTO_CREATE, "true")
    //Tells elastisearch-hadoop what field to use as the ID to formulate the correct ES API calls
    jobConf.set(ConfigurationOptions.ES_MAPPING_ID, "_id")
    //This tells elasticsearch-hadoop what field represents the elasticsearch type of the record
    jobConf.set(ConfigurationOptions.ES_RESOURCE_WRITE, index + "/{ingestType}")
    //Since we're not running an actual Hadoop job, these only serve to turn off some warning log statements
    jobConf.set("mapreduce.map.speculative","false")
    jobConf.set("mapreduce.reduce.speculative","false")

    //The saveAsHadoopDataset call wants a PairRDD, so we build one here
    //I'm pretty sure the elasticsearch-hadoop api ignores the ID field and only looks at the document, though
    val es: RDD[(String, String)] = rawData.map(
      row => (
        row.getAs[String]("id"),
        row.getAs[String]("json_document")
      )
    )

    //This actually kicks off the save process
    es.saveAsHadoopDataset(jobConf)

    //This cleans up the Spark connection
    spark.stop()
  }
}
