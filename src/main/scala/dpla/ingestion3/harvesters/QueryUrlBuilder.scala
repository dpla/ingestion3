package dpla.ingestion3.harvesters

import java.net.URL
import org.apache.http.client.utils.URIBuilder

/**
  * Creates a fully formed and valid URL for OAI request
  */
class OaiQueryUrlBuilder extends QueryUrlBuilder with Serializable {
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
    assume(params.get("endpoint").isDefined)
    assume(params.get("verb").isDefined)
    val url = new URL(params("endpoint"))
    val verb = params("verb")

    // Optional properties.
    val metadataPrefix: Option[String] = params.get("metadataPrefix")
    val resumptionToken: Option[String] = params.get("resumptionToken")
    val set: Option[String] = params.get("set")

    // Build the URL
    val urlParams = new URIBuilder()
      .setScheme(url.getProtocol)
      .setHost(url.getHost)
      .setPort(url.getPort)
      .setPath(url.getPath)
      .setParameter("verb", verb)

    if(resumptionToken.isDefined) {
      urlParams.setParameter("resumptionToken", resumptionToken.get)
    }

    /*
     * Set `set' param if:
     *   - set is defined
     *   - there is no resumption token (if an OAI call with a resumption token
     *     also contains a set, an OAI badArgument error is returned).
     */
    if (!resumptionToken.isDefined) {
      set.foreach(s => urlParams.setParameter("set", s))
    }

    /*
     * Set metadataPrefix param if:
     *   - metadataPrefix is defined
     *   - there is no resumption token (if an OAI call with a resumption token
     *     also contains a metadataPrefix, an OAI badArgument error is returned).
     */
    if (!resumptionToken.isDefined) {
      metadataPrefix.foreach(prefix => urlParams.setParameter("metadataPrefix", prefix))
    }

    urlParams.build.toURL
  }
}

class ResourceSyncUrlBuilder extends QueryUrlBuilder with Serializable {

  /**
    *
    * @param params
    * @return
    */
  def buildQueryUrl(params: Map[String, String]): URL = {
    assume(params.get("endpoint").isDefined)

    val url = new URL(params.get("endpoint").get)

    // Build the URL
    val urlParams = new URIBuilder()
      .setScheme(url.getProtocol)
      .setHost(url.getHost)
      .setPath(url.getPath)

    params.get("path") match {
      case Some(p) => urlParams.setPath(p)
      case _ => None // do nothing
    }

    params.get("accept") match {
      case Some(p) => urlParams.setParameter("Accept", p)
      case _ =>
    }

    urlParams.build().toURL
  }
}

/**
  *
  */
trait QueryUrlBuilder {
  def buildQueryUrl (params: Map[String, String]): URL

}
