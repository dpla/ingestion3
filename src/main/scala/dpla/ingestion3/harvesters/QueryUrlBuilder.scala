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
    assume(params.get("metadataPrefix").isDefined)

    /*
     * The following OAI verbs require a metadataPrefix argument.
     * If an OAI call contains a verb NOT in this list AND a metadataPrefix,
     * a badArgument errors is returned.
     *
     * @see https://www.openarchives.org/OAI/openarchivesprotocol.html#ProtocolMessages
     */
    val verbsWithMetadataPrefix = List("GetRecord", "ListIdentifiers", "ListRecords")

    val verb = params("verb")
    val metadataPrefix = params("metadataPrefix")
    val url = new URL(params("endpoint"))
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
    if (set.isDefined && !resumptionToken.isDefined) {
      urlParams.setParameter("set", set.get)
    }

    /*
     * Set metadataPrefix param if:
     *   - the verb allows a metadataPrefix argument AND
     *   - there is no resumption token (if an OAI call with a resumption token
     *     also contains a metadataPrefix, an OAI badArgument error is returned).
     */
    if(verbsWithMetadataPrefix.contains(verb) && !resumptionToken.isDefined) {
      urlParams.setParameter("metadataPrefix", metadataPrefix)
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
