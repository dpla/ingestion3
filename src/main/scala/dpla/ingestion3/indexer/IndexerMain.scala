package dpla.ingestion3.indexer

import dpla.ingestion3.utils.FlatFileIO
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark


object IndexerMain {

  def main(args:Array[String]): Unit = {

    val input = args(0)
    val esCluster = args(1)
    val esPort = args(2)
    val index = args(3)
    val schema = new FlatFileIO().readFileAsString("/avro/IndexRecord_MAP3.avsc")

    val filter: Option[String] = if (args.isDefinedAt(4)) Some(args(4)) else None

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

    /**
      * Select only those rows that contain the `filter` substring anywhere
      * within the `document` String.
      */
    val filteredData: RDD[Row] = {
      filter match {
        case Some(filter) => rawData.filter(_.getAs[String]("document").contains(filter))
        case None => rawData
      }
    }

    //This means we're giving it json instead of ES API record objects:
    conf.set("es.output.json", "yes")
    //This is the host name of the Elasticsearch cluster we're writing to
    conf.set("es.nodes", esCluster)
    //This is the port number that Elasticsearch listens on
    conf.set("es.port", esPort)
    //This tells it to create the index if it doesn't exist.
    conf.set("es.index.auto.create", "yes")
    //Tells elastisearch-hadoop what field to use as the ID to formulate the correct ES API calls
    conf.set("es.mapping.id", "id")

    val rdd: RDD[String] = filteredData.map(
      row => {
        val doc: String = row.getAs[String]("document")
        // We remove the _id property of the document because Elasticsearch 5
        // rejects it.  This should be fine, because we make no promises to
        // users about its existence or utility. In fact,
        // https://dp.la/info/developers/codex/responses/field-reference/ says
        // for all "_" fields, "Internal field -- look away."
        val fixedDoc: String = doc.replaceFirst(""""_id":\s*".+?",""", "")
        fixedDoc
      }
    )

    /*
     * FIXME: All records are updated as items, but some will be collections and
     * we need to add a schema for collections as we have done in Ingestion 1.
     */
    EsSpark.saveJsonToEs(rdd, index + "/item")

    //This cleans up the Spark connection
    spark.stop()
  }
}
