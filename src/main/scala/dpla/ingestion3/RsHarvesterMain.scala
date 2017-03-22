package dpla.ingestion3

import java.io.File

import dpla.ingestion3.harvesters.resourceSync.{ResourceSyncProcessor, ResourceSyncRdd}
import dpla.ingestion3.utils.Utils
import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}



// Example usage:
// PATH_TO_SPARK/bin/spark-submit --class "la.dp.ingestion3.RsHarvesterMain" \
//   --master local[3] \
//   PATH_TO_INGESTION3_APP/target/scala-2.11/________.jar \
//   OUTPUT_PATH


object RsHarvesterMain extends App {

  val logger = LogManager.getLogger(RsHarvesterMain.getClass)

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
     .setMaster("local[32]") // TODO -- parameterize
  val sc = new SparkContext(sparkConf)


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
      new ResourceSyncRdd(resourceItems, sc)
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

  // Save to text file
  rsRdd.saveAsTextFile(outputFile)


  // Timing end and print results
  val end = System.currentTimeMillis()
  val recordCount = rsRdd.count()
  Utils.printRuntimeResults(end-start, recordCount)
}
