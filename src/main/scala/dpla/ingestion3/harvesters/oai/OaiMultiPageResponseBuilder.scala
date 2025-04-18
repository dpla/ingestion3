package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder
import org.apache.logging.log4j.LogManager

import java.net.URL

class OaiMultiPageResponseBuilder(
    endpoint: String,
    verb: String,
    metadataPrefix: Option[String] = None,
    set: Option[String] = None,
    sleep: Int = 0
) extends Serializable {

  private val logger = LogManager.getLogger(this.getClass)

  /** Main entry point. Get all pages of results from an OAI feed. OaiPages may
    * contain an OAI error (HTTP code 200) or invalid XML.
    *
    * Makes an initial call to the feed to get the first page of results. For
    * this and all subsequent pages, calls the next page if a resumption token
    * is present.
    *
    * @return
    *   Un-parsed response page from OAI requests, including OaiPages and
    *   OaiErrors.
    */
  def getResponse: Iterable[OaiPage] =
    new Iterable[OaiPage] {
      override def iterator: Iterator[OaiPage] = new Iterator[OaiPage]() {

        def handleErrors(eitherResponse: Either[OaiError, OaiPage], url: URL, requestInfo: OaiRequestInfo): Option[OaiPage] = eitherResponse match {
          case Left(error) =>
            error match {
              case NoRecordsMatch(requestInfo) =>
                None // Not an error that should interrupt execution.
              case _ =>
                throw new RuntimeException(s"OAI Error: $error for URL $url and request info: $requestInfo")
            }
          case Right(page) =>
            Some(page)
         }

        var onDeck: Option[OaiPage] = {
          val requestInfo = OaiRequestInfo(verb=verb, metadataPrefix=metadataPrefix, set=set, resumptionToken = None, timestamp=System.currentTimeMillis())
          val url = buildUrl()
          val page = getSinglePage(url, requestInfo)
          handleErrors(page, url, requestInfo)
        }

        override def hasNext: Boolean = onDeck.isDefined

        override def next(): OaiPage = onDeck match {
          case None =>
            throw new RuntimeException("Called next() on end of iterator.")
          case Some(last) =>
            OaiXmlParser.getResumptionToken(
              OaiXmlParser.parsePageIntoXml(last)
            ) match {
              case None => onDeck = None
              case Some(tokenValue) =>
                val requestInfo = last.info.copy(resumptionToken = Some(tokenValue), timestamp=System.currentTimeMillis())
                val url = buildUrl(Some(tokenValue))
                onDeck = handleErrors(getSinglePage(url, requestInfo), url, requestInfo)
            }
            last
        }
      }
    }

  private val OAI_ERROR_PATTERN = """<error.*>.*</error>""".r

  /** Get a single-page, un-parsed String response from the OAI feed, or an
    * error if one occurs.
    *
    * The page may contain an OAI error (HTTP code 200) or invalid XML.
    *
    * @param url
    *   URL for a single OAI request
    * @return
    *   OaiPage
    */

  def getSinglePage(url: URL, requestInfo: OaiRequestInfo): Either[OaiError, OaiPage] = {
    if (sleep > 0)
      Thread.sleep(sleep)
    logger.info("Loading page {}: {}", url.toString, requestInfo)
    val page = HttpUtils.makeGetRequest(url)
    if (OAI_ERROR_PATTERN.matches(page)) {
      val xml = OaiXmlParser.parsePageIntoXml(OaiPage(page, requestInfo))
      val errorCode = xml \ "error" \@ "code"
      Left(OaiError.errorForCode(errorCode, requestInfo))
    } else {
      Right(OaiPage(page, requestInfo))
    }
  }

  /** Builds an OAI request. Endpoint and verb must be present in all requests,
    * including those containing a resumptionToken.
    *
    * Set and metadataPrefix can only be present in a first request, and cannot
    * be present in any request that contains a resumptionToken.
    *
    * @see
    *   https://www.openarchives.org/OAI/openarchivesprotocol.html#ProtocolMessages
    * @param resumptionToken
    *   An OAI resumption token
    * @return
    *   Try[URL]
    */
  def buildUrl(resumptionToken: Option[String] = None): URL = {

    val url = new URL(endpoint)

    val urlParams = new URIBuilder()
      .setScheme(url.getProtocol)
      .setHost(url.getHost)
      .setPort(url.getPort)
      .setPath(url.getPath)
      .setParameter("verb", verb)

    // Set optional parameters.
    resumptionToken match {
      case Some(t) => urlParams.setParameter("resumptionToken", t)
      case None =>
        set.foreach(s => urlParams.setParameter("set", s))
        metadataPrefix.foreach(p => urlParams.setParameter("metadataPrefix", p))
    }

    urlParams.build.toURL
  }

}
