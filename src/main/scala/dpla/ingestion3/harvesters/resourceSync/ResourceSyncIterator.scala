package dpla.ingestion3.harvesters.resourceSync

import java.net.URL

import dpla.ingestion3.harvesters.ResourceSyncUrlBuilder

import scala.xml._
/**
  * Expects an endpoint
  *
  * @param
  */
class ResourceSyncIterator (queryUrlBuilder: ResourceSyncUrlBuilder) {

  def makeRequest(params: Map[String,String]): Elem = {
    // .well-known/resourcesync
    val qUrl = queryUrlBuilder.buildQueryUrl(params)

    println(qUrl.toString)
    XML.load(qUrl)
  }

  /**
    * Expects the capabilitylist list URL and returns a list of the capabilities of the endpoint
    *
    * @param url
    * @return Map of the capability term (see: https://www.openarchives.org/rs/1.1/resourcesync#CapabilityList) and
    *         the corresponding URL
    */
  def getCapibilityUrls(url: URL): Map[String, String] = {
    val rsp = XML.load(url)
    (rsp \\ "url").map( u => {
      (u \\ "@capability").text -> (u \\ "loc").text
    }).toMap
  }

  /**
    * Accepts the 'well-known' URL and returns the capabilities URL
    *
    * @param url
    * @return
    */
  def getCapabilityListUrl(url: URL): Option[URL] = {
    val rsp = XML.load(url)
    (rsp \\ "url").map( u => {
      u \\ "@capability" text match {
        case "capabilitylist" => {
          Some(new URL(u \ "loc" text))
        }
        case _ => None
      }
    }).head // Head is required to not return a Seq(Option[URL])
  }
}
