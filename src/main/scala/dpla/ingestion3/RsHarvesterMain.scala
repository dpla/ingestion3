package dpla.ingestion3

import dpla.ingestion3.harvesters.ResourceSyncUrlBuilder
import dpla.ingestion3.harvesters.resourceSync.ResourceSyncIterator

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
    case (true, false) => {
      // Full sync using ResourceList
      println("Do it using ResourceList")
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
    case _ => throw new Exception("This is strange")
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
