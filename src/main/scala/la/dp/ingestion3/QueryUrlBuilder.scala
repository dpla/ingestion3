package la.dp.ingestion3

import java.net.URL

import org.apache.http.client.utils.URIBuilder

/**
  * Creates a fully formed and valid URL for OAI request
  */
class OaiQueryUrlBuilder extends QueryUrlBuilder {
  /**
    * Builds an OAI request
    *
    * @param params Map of parameters needed to construct the URL
    *               OAI request verbs
    *               See https://www.openarchives.org/OAI/openarchivesprotocol.html#ProtocolMessages
    * @return URL
    */
  def buildQueryUrl(params: Map[String, String]): URL = {
    // Required properties, not sure if this is the right style
    assume(params.get("endpoint").get.nonEmpty)
    assume(params.get("verb").get.nonEmpty)
    assume(params.get("metadataPrefix").get.nonEmpty)

    val verb = params.get("verb").get
    val metadataPrefix = params.get("metadataPrefix").get
    val url = new URL(params.get("endpoint").get)

    // Build the URL
    val urlParams = new URIBuilder()
      .setScheme(url.getProtocol)
      .setHost(url.getHost)
      .setPath(url.getPath)
      .setParameter("verb", verb)

    params.get("resumptionToken") match {
      case Some(value) => urlParams.setParameter("resumptionToken", value)
      case None  => urlParams.setParameter("metadataPrefix", metadataPrefix)
    }

    urlParams.build.toURL
  }
}

trait QueryUrlBuilder {
  def buildQueryUrl (params: Map[String, String]): URL

}
