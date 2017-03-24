package dpla.ingestion3

import java.io.File

import com.databricks.spark.avro._
import dpla.ingestion3.harvesters.resourceSync.{ResourceSyncProcessor, ResourceSyncRdd}
import dpla.ingestion3.utils.Utils
import org.apache.avro.Schema
import org.apache.log4j.LogManager
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf


/**
  * Invocation to run on EMR:
  *
  * spark-submit
  *   --packages com.databricks:spark-avro_2.11:3.2.0
  *   --class "dpla.ingestion3.RsHarvesterMain"
  *   --master yarn
  *   <BUILD_JAR_NAME>.jar
  *   s3n://<PATH>
  */


object RsHarvesterMain extends App {

  val logger = LogManager.getLogger(RsHarvesterMain.getClass)

  val schemaStr =
    """{
        "namespace": "la.dp.avro",
        "type": "record",
        "name": "OriginalRecord.v1",
        "doc": "",
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "provider", "type": "string"},
          {"name": "document", "type": "string"},
          {"name": "mimetype", "type": { "name": "MimeType",
           "type": "enum", "symbols": ["application_json", "application_xml", "text_turtle"]}
           }
        ]
      }
    """//.stripMargin // TODO we need to template the document field so we can record info there


  // Complains about not being typesafe...
  if(args.length != 1 ) {
    logger.error("Bad number of args: <OUTPUT FILE>")
    sys.exit(-1)
  }

  val baselineSync = true
  val isDumpFuncSupported = false // TODO placeholder for function to make determination
  val outputFile = args(0)
  val hyboxResourceList = Some("https://hyphy.demo.hydrainabox.org/resourcelist")

  Utils.deleteRecursively(new File(outputFile))

  val sparkConf = new SparkConf()
    .setAppName("Hydra Resource Sync")
    // .setMaster("local[32]") // TODO -- parameterize

  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  val sc = spark.sparkContext


  // Timing start
  val start = System.currentTimeMillis()
  /**
    * There are four possible ways to harvest from a ResourceSync endpoint depending on wether a full or partial sync
    * is being performed and whether *Dump functionality is supported.
    *
    */
  val rsRdd: ResourceSyncRdd = (baselineSync, isDumpFuncSupported) match {
    case (true, false)=> {
      // Perform full sync using ResourceList
      /**
        * This is what needs to work for Hybox initial test
        *
        *
        * Additional notes on Hydra endpoint testing
        * ----------------------------------
        *   Not currently implemented by this endpoint
        *     1. Resource List Index
        *     2. ResourceDump
        *     3. ChangeList
       */
      // Returns a Sequence of the items resources that need to be fetched
      val resourceItems = ResourceSyncProcessor.getResources(hyboxResourceList)
      new ResourceSyncRdd(resourceItems, sc).persist(StorageLevel.DISK_ONLY)
    }

    /*
    TODO These cases need to be implemented but not in scope for initial hybox tests

    case (true, true) => {
      // Perform full sync using ResourceDump
    }
    case (false, true) => {
      // Perform partial sync of changes using ChangeDump
    }
    case (false, false) => {
      // Perform partial sync of changes using ChangeList
    }
    */

    case _ => throw new Exception("This is strange...")
  }

  // From OAI harvester
  val avroSchema = new Schema.Parser().parse(schemaStr)
  val schemaType = SchemaConverters.toSqlType(avroSchema)
  val structSchema = schemaType.dataType.asInstanceOf[StructType]

  // Map harvested data and creates a DataFrame
  // TODO Remove hard coded provider value
  val rows = rsRdd.map(data => { Row(data._1, "hybox", data._2, "text_turtle") })
  val dataframe = spark.createDataFrame(rows, structSchema)

  // Save dataframe to avro file
  dataframe.write.format("com.databricks.spark.avro").option("avroSchema", schemaStr).save(outputFile)


  // Timing end and print results
  val end = System.currentTimeMillis()
  val recordCount = dataframe.count()
  Utils.printRuntimeResults(end-start, recordCount)
}
