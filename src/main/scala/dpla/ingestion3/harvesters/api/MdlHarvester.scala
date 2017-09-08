
package dpla.ingestion3.harvesters.api

import java.net.URI

import org.apache.commons.io.IOUtils
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

import scala.util.{Failure, Success, Try}

/**
  * Class for harvesting records from the Minnesota Digital Library's API
  *
  * @param queryParams - Query parameters (q, rows, cursorMark etc.)
  */
class MdlHarvester(val queryParams: Map[String, String] = Map()) {
  /**
    * Get a single-page, un-parsed response from the API feed, or an error if
    * one occurs.
    *
    * @param start Uses start as an offset to paginate.
    * @return ApiSource or ApiError
    */
  def harvest(start: String): ApiResponse = {
    getUri(queryParams.updated("start", start)) match {
      // Error building URL
      case Failure(e) =>
        val source = ApiSource(queryParams)
        ApiError(e.toString, source)
      case Success(url) => {
        getResponse(url) match {
          case Failure(e) =>
            ApiError(e.toString, ApiSource(queryParams, Some(url.toString)))
          case Success(response) => response.isEmpty match {
            case true => ApiError("Response body is empty", ApiSource(queryParams, Some(url.toString)))
            case false => ApiSource(queryParams, Some(url.toString), Some(response))
          }
        }
      }
    }
  }

  /**
    * Builds query URI from parameters
    *
    * @param queryParams
    * @return URI
    */
  def getUri(queryParams: Map[String, String]): Try[URI] = Try {
    new URIBuilder()
      .setScheme("http")
      .setHost("hub-client.lib.umn.edu")
      .setPath("/api/v1/records")
      .setParameter("q", queryParams.getOrElse("query", "*:*"))
      .setParameter("start", queryParams.getOrElse("start", "0"))
      .setParameter("rows", queryParams.getOrElse("rows", "10"))
      .build()
  }

  /**
    * Makes request and returns stringified JSON response
    *
    * @param uri
    * @return
    */
  def getResponse(uri: URI): Try[String] = Try {
    val httpclient = HttpClients.createDefault()
    val get = new HttpGet(uri)
    var response: CloseableHttpResponse = null
    try {
      response = httpclient.execute(get)
      val entity = response.getEntity
      EntityUtils.toString(entity)
    } finally {
      IOUtils.closeQuietly(response)
    }
  }
}
