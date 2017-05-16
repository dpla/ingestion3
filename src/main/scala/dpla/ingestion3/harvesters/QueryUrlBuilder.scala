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

    val verb = params("verb")
    val metadataPrefix = params("metadataPrefix")
    val url = new URL(params("endpoint"))

    // Build the URL
    val urlParams = new URIBuilder()
      .setScheme(url.getProtocol)
      .setHost(url.getHost)
      .setPath(url.getPath)
      .setParameter("verb", verb)

    params.get("resumptionToken") match {
      case Some(v) if v.nonEmpty => setResumptionParams(v)
      case _ => setInitialRequestParams
    }

    /**
      * Set the params that should ONLY be included in an initial OAI request.
      * These params should NOT be included in a resumption request.
      */
    def setInitialRequestParams = {
      urlParams.setParameter("metadataPrefix", metadataPrefix)

      params.get("set") match {
        case Some(v) if v.nonEmpty => urlParams.setParameter("set", v)
        case _ => urlParams
      }
    }

    /**
      * Set the resumption token param.
      */
    def setResumptionParams(resumptionToken: String) = {
      urlParams.setParameter("resumptionToken", resumptionToken)
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
