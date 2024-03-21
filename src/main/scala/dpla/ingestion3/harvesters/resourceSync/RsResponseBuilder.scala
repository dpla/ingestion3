package dpla.ingestion3.harvesters.resourceSync

import java.net.URL

import dpla.ingestion3.utils.HttpUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import scala.annotation.tailrec
import scala.util.{Failure, Success}
import scala.xml.XML

/** This class handles requests to the ResourceSync feed. It partitions data per
  * resourceList.
  */

class RsResponseBuilder(endpoint: String)(@transient val sqlContext: SQLContext)
    extends Serializable {

  /** Gets all the records described by the source
    *
    * TODO: We can make better us of our ability to parallelize here by
    * partitioning a sequence of resource URLs and then mapping them to HTTP
    * responses. That would be more efficient than doing the HTTP request
    * sequentially.
    *
    * @return
    *   RDD[RsResponse]
    */
  def getAllResourceList(): RDD[RsResponse] = {
    val multiPageResponse = getMultiResourceResponse()
    sqlContext.sparkContext.parallelize(multiPageResponse)
  }

  /** Expects the capabilitylist list URL and returns a list of the capabilities
    * of the endpoint
    *
    * @see
    *   https://www.openarchives.org/rs/1.1/resourcesync#CapabilityList
    * @param capUrl
    *   URL that describes the capabilities of the source
    * @return
    *   Map of the capability term and it's corresponding URL
    */
  def getCapibilityUrls(capUrl: URL): Map[String, String] = {
    val rsp = XML.load(capUrl)
    (rsp \\ "url")
      .map(u => {
        (u \\ "@capability").text -> (u \\ "loc").text
      })
      .toMap
  }

  /** Take the capability URL and extracts all of the resource lists defined
    *
    * TODO This should support Sitemaps in addition to RS capabilities
    * @see
    *   https://www.openarchives.org/rs/1.1/resourcesync#Walkthrough
    *
    * @param url
    *   Capability URL
    * @return
    *   Sequence of resource list urls
    */
  def getResourceLists(url: URL): Seq[String] = {
    val rsp = XML.load(url)
    (rsp \\ "url")
      .map(u => {
        (u \\ "loc").text -> (u \\ "@capability").text
      })
      .filter(pairs =>
        pairs match {
          case (_, "resourcelist") => true
          case (_, _)              => false
        }
      )
      .map { case (r, _) => r }
  }

  /** Loads XML from URL and extracts all values in <loc> properties
    *
    * @param url
    * @return
    *   Sequence of strings
    */
  def getUrls(url: URL): Seq[String] = {
    val rsp = XML.load(url)
    (rsp \\ "loc").map(r => r.text)
  }

  /** Parses XML string and extracts all values in <loc> properties
    *
    * @param xmlStr
    *   String value representing XML
    * @return
    *   Sequence of URLs
    */
  def getUrls(xmlStr: String): Seq[String] = {
    val rsp = XML.loadString(xmlStr)
    (rsp \\ "loc").map(r => r.text)
  }

  /** @return
    */
  def getMultiResourceResponse(): List[RsResponse] = {
    // Request the capability/page that describes resourceLists / resourceDumps
    val resourceLists = getResourceLists(new URL(endpoint))

    // validates
    resourceLists.headOption.getOrElse(
      throw new RsHarvesterException(
        s"resourcelist capability not supported by endpoint ${endpoint}"
      )
    )

    @tailrec
    def loop(data: List[RsResponse], lists: Seq[String]): List[RsResponse] = {
      data.headOption match {
        // Stops the harvest if an RsError or Http error was trapped and returns everything
        // harvested up that this point plus the error
        case Some(error: RsError) => data
        // If it was a valid and parsable response then extract data and call the next resourceList page
        case Some(previous: RsSource) =>
          val nextList = lists.lastOption
          nextList match {
            case None => data
            case Some(listPage) =>
              val nextResponse = getResources(listPage)
              loop(nextResponse ::: data, lists.init)
          }
        // This is only reached if something really strange happened
        // If there is an error or unexpected response type, return all data
        // collected up to this point (including the error or unexpected response).
        case _ => data
      }
    }

    val firstList = resourceLists.lastOption.getOrElse(
      throw RsHarvesterException("Empty resourceList. Nothing to harvest")
    )
    val firstData = getResources(firstList)

    loop(firstData, resourceLists.init)

  }

  /** @param resourceListUrl
    * @return
    */
  def getResources(resourceListUrl: String): List[RsResponse] = {
    val headers = Map("Accept" -> "text/json")
    HttpUtils.makeGetRequest(new URL(resourceListUrl), Some(headers)) match {
      case Success(indexDoc) => {
        val items = getUrls(indexDoc)
        items
          .map(i => {
            val url = new URL(i)
            HttpUtils.makeGetRequest(url, Some(headers)) match {
              case Success(item) => RsRecord("id", item, RsSource(Some(i)))
              case Failure(error) =>
                RsError(error.getMessage, RsSource(Some(i)))
            }
          })
          .toList
      }
      case Failure(f) => {
        List(
          RsError(
            s"Unable to get resource list page ${f.getMessage}",
            RsSource(Some(resourceListUrl))
          )
        )
      }
    }
  }
}
