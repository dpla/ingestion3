package la.dp.ingestion3

import java.net.URL

import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.LogManager

/**
  * Created by Scott on 1/31/17.
  */
class OaiQueryUrlBuilder extends QueryUrlBuilder {

  // Temporary
  private[this] val logger = LogManager.getLogger("Query URL Builder ")

  /**
    * Builds an OAI request
    * @param params Map
    *             OAI request verb
    *             See https://www.openarchives.org/OAI/openarchivesprotocol.html#ProtocolMessages
    * @return URL
    */
  def buildQueryUrl(params: Map[String, String]): URL = {
    assume(params.getOrElse("endpoint","").nonEmpty)
    assume(params.getOrElse("metadataPrefix","").nonEmpty)
    assume(params.getOrElse("verb", "").nonEmpty)

    val url = new URL(params.getOrElse("endpoint", ""))

    // Build the URL parameters
    val urlParams = new URIBuilder()
      .setScheme(url.getProtocol)
      .setHost(url.getHost)
      .setPath(url.getPath)
      .setParameter("verb", params.getOrElse("verb", ""))

    // If resumptionToken is empty then use the metadataPrefix parameter
    params.get("resumptionToken") match {
      case Some(value) => urlParams.setParameter("resumptionToken", value.toString)
      case None  => urlParams.setParameter("metadataPrefix", params.getOrElse("metadataPrefix", ""))
    }
    urlParams.build.toURL
  }
}

// TODO learn how to properly override this w/o listing all params
trait QueryUrlBuilder {
  def buildQueryUrl (params: Map[String, String]): URL

}
