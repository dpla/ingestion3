package dpla.ingestion3

import dpla.ingestion3.harvesters.ResourceSyncUrlBuilder
import dpla.ingestion3.harvesters.resourceSync.ResourceSyncIterator
import dpla.ingestion3.utils.ResourceSyncRdd
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by scott on 3/16/17.
  */
object RsHarvesterMain extends App {

  // Vars
  val urlBuilder = new ResourceSyncUrlBuilder()
  val rsIter = new ResourceSyncIterator(urlBuilder)
  // TODO this should be an option or programatically determined
  val baselineSync = true
  val outputFile = "/home/scott/Desktop"
  val endpoint = "https://hyphy.demo.hydrainabox.org"

  // ResourceSync paths
  val WELL_KNOWN_PATH = "/.well-known/resourcesync"

  val WELL_KNOWN_URL = urlBuilder.buildQueryUrl( Map("endpoint"->endpoint,"path"->WELL_KNOWN_PATH))
  val CAPABILITIES_URL = rsIter.getCapabilityListUrl(WELL_KNOWN_URL)

  /*
  Get the capabilities of the ResourceSync endpoint. This needs to happen so we know whether to use Dump or List
   when picking up changes or getting baseline
   */
  val capabilities = CAPABILITIES_URL match {
    case Some(c) => rsIter.getCapibilityUrls(c)
    case _ => throw new Exception("W/o capabilities there isn't much to do.")
  }


  /**
    * There are four possible ways to harvest from a ResourceSync endpoint and this match determines which one
    * should be invoked
    */
  (baselineSync, isDumpSupported(baselineSync)) match {
    case (true, false)=> {
      // Full sync using ResourceList
      // Requires the URL paried with capability="resourcelist"

      /*
        * TODO this is what needs to work for hybox initial test
        * Notes on hydra testing --
        *   + Not currently implemented at source, Resource List Index
        *     Resource Dump, Change Dump.
       */

      println("Do it using ResourceList")
      val resourcelist_url = capabilities.get("resourcelist") match {
        case Some(u) => u.toString
        case _ => throw new Exception("No resources to get.") // log error
      }

      val sparkConf = new SparkConf().setAppName("Hydra Resource Sync")
      val sc = new SparkContext(sparkConf)
      val crdd = new ResourceSyncRdd(resourcelist_url, sc)
      crdd.saveAsTextFile("/home/scott/hydra-harvest.txt")
      sc.stop()
    }
    case (true, true) => {
      // Fully sync using ResourceDump
    }
    case (false, true) => {
      // Sync changes using ChangeDump
    }
    case (false, false) => {
      // Sync changes using ChangeList
    }
    case _ => throw new Exception("This is strange...")
  }


  /**
    * Checks whether the "Dump" functionality is supported by the endpoint for the type of sync being
    * performed (compete vs partial)
    *
    * @param baselineSync
    * @return
    */
  def isDumpSupported(baselineSync: Boolean): Boolean = {
    baselineSync match {
      case true => capabilities.contains("resourcedump")
      case false => capabilities.contains("changedump")
      case _ => false
    }
  }

}
