package dpla.ingestion3

import java.io._

import com.databricks.spark.avro._
import org.apache.avro.Schema
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.eclipse.rdf4j.model.Model
import org.eclipse.rdf4j.model.util.ModelBuilder
import org.eclipse.rdf4j.rio._
import dpla.ingestion3.utils.Utils
import dpla.ingestion3.enrichments.Spatial

/**
 * Driver for running an enrichment
 *
 * This is a prototype that is hardcoded to run the spatial enrichment,
 * SpatialEnricher.
 *
 * See https://digitalpubliclibraryofamerica.atlassian.net/wiki/display/TECH/Ingestion+3+Storage+Specification#Ingestion3StorageSpecification-S3BucketFolderStructureandContents
 * with suggestions for how to organize input and output locations. 
 *
 * Example invocation:
 * $ sbt "run-main dpla.ingestion3.EnricherMain local[2] <from> <to>"
 * ... where <from> is the Avro resource (file / directory) and <to> is the
 *     destination (directory or s3 folder)
 *
 */
object EnricherMain {

  // Avro schema for our output
  val schemaStr: String =
    """
    {
      "namespace": "la.dp.avro",
      "type": "record",
      "name": "MAPRecord.v1",
      "doc": "Prototype enriched record",
      "fields": [
        {"name": "id", "type": "string"},
        {"name": "document", "type": "string"}
      ]
    }
    """
  val logger = LogManager.getLogger(EnricherMain.getClass)

  /*
   * args:
   *   master: String -- Spark master descriptor
   *   inputURI: String -- URI of Avro input
   *   outputURI: String -- URI of Avro output
   */
  def main(args: Array[String]) {

    if (args.length != 3) {
      logger.error("Arguments should be <master>, <inputURI>, <outputURI>")
      sys.exit(-1)
    }
    val sparkMaster = args(0)
    val inputURI = args(1)
    val outputURI = args(2)

    val start_time = System.currentTimeMillis()

    val sparkConf = new SparkConf()
      .setAppName("Enricher")
      .setMaster(sparkMaster)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val rdd: RDD[Row] = spark.read
      .format("com.databricks.spark.avro")
      .option("avroSchema", schemaStr)
      .load(inputURI)
      .rdd

    // Process all of the records and wrap them with a dataframe
    val rows = rdd.map(
      record => (
        record.getAs[String]("id"),
        process(record.getAs[String]("document"))
      )
    )
    // The dataframe is created with fields named "id" and "document" which is
    // not as nice as actually embedding the schema that we've defined in
    // schemaStr. I've tried to embed that using
    // .options("avroSchema", schemaStr) below in the call to dataframe.write(),
    // but that silently does nothing.
    // -mb
    val dataframe = spark.createDataFrame(rows).toDF("id", "document")

    val recordCount = dataframe.count()
    Utils.deleteRecursively(new File(outputURI))
    dataframe.write
             .format("com.databricks.spark.avro")
             .avro(outputURI)
    sc.stop()

    val end_time = System.currentTimeMillis()
    Utils.printResults((end_time - start_time), recordCount)

  }

  def process(rdfString: String): String = {
    try {
      val rdfByteStream = new StringReader(rdfString)
      val in_model = Rio.parse(rdfByteStream, "", RDFFormat.TURTLE)
      val out_model: Model = Spatial.enrich(in_model)
      // out_model could be passed to other enrichments in turn
      val out = new StringWriter()
      Rio.write(out_model, out, RDFFormat.TURTLE)
      out.toString
    } catch {
      case e: Exception => {
        val sw = new StringWriter
        val pw = new PrintWriter(sw)
        e.printStackTrace(pw)
        logger.warn(sw.toString)
        sw.toString
      }
    }
  }

}
