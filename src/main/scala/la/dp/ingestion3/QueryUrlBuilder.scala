package la.dp.ingestion3

import java.net.URL

import org.apache.http.client.utils.URIBuilder

/**
  * Created by scott on 1/31/17.
  */
class OaiQueryUrlBuilder extends QueryUrlBuilder {
  /**
    * Builds an OAI request
    *
    * @param resumptionToken String
    *                        Optional, token used to resume a previous harvest request
    * @param verb String
    *             OAI request verb
    *             See https://www.openarchives.org/OAI/openarchivesprotocol.html#ProtocolMessages
    * @return URL
    */
  def buildQueryUrl( endpoint: URL,
    metadataPrefix: String,
    resumptionToken: String = "",
    verb: String): URL = {

    // Build the URL parameters
    val urlParams = new URIBuilder()
      .setScheme(endpoint.getProtocol)
      .setHost(endpoint.getHost)
      .setPath(endpoint.getPath)
      .setParameter("verb", verb)

    // If resumptionToken is empty then use the metadataPrefix parameter
    resumptionToken match {
      case "" => urlParams.setParameter("metadataPrefix", metadataPrefix)
      case _  => urlParams.setParameter("resumptionToken", resumptionToken)
    }
    urlParams.build.toURL
  }
}

// TODO learn how to properly override this w/o listing all params
trait QueryUrlBuilder {
  def buildQueryUrl(endpoint: URL,
                    metadataPrefix: String,
                    resumptionToken: String = "",
                    verb: String): URL
}
