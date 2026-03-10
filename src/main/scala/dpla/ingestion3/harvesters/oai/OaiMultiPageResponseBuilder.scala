package dpla.ingestion3.harvesters.oai

import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder
import org.apache.logging.log4j.LogManager

import java.net.URL
import scala.util.{Failure, Success, Try}

class OaiMultiPageResponseBuilder(
    endpoint: String,
    verb: String,
    metadataPrefix: Option[String] = None,
    set: Option[String] = None,
    sleep: Int = 0,
    harvestLogger: OaiHarvestLogger = OaiHarvestLogger.Noop
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

        def handleErrors(
            eitherResponse: Either[OaiError, OaiPage],
            url: URL,
            requestInfo: OaiRequestInfo
        ): Option[OaiPage] = eitherResponse match {
          case Left(error) =>
            error match {
              case NoRecordsMatch(_) =>
                None // Not an error that should interrupt execution.
              case _ =>
                throw new OaiHarvestException(
                  requestInfo = requestInfo,
                  url = url.toString,
                  stage = "oai_error",
                  oaiError = Some(error),
                  cause = new RuntimeException(s"OAI Error: $error")
                )
            }
          case Right(page) =>
            Some(page)
        }

        var onDeck: Option[OaiPage] = {
          val requestInfo = OaiRequestInfo(
            verb = verb,
            metadataPrefix = metadataPrefix,
            set = set,
            resumptionToken = None,
            timestamp = System.currentTimeMillis()
          )
          val url = buildUrl()
          harvestLogger.logPageRequest(requestInfo, url.toString)
          val page = getSinglePage(url, requestInfo)
          page match {
            case Right(_) =>
              harvestLogger.logPageSuccess(requestInfo, url.toString)
            case Left(err) =>
              harvestLogger.logPageFailure(
                requestInfo,
                url.toString,
                err.toString
              )
          }
          handleErrors(page, url, requestInfo)
        }

        override def hasNext: Boolean = onDeck.isDefined

        override def next(): OaiPage = onDeck match {
          case None =>
            throw new RuntimeException("Called next() on end of iterator.")
          case Some(last) =>
            Try {
              val xml = OaiXmlParser.parsePageIntoXml(last)
              OaiXmlParser.getResumptionTokenWithAttrs(xml)
            } match {
              case Failure(e) =>
                val (firstId, lastId) =
                  OaiXmlParser.extractIdentifiers(last.page)
                harvestLogger.logPageFailure(last.info, endpoint, e.getMessage)
                throw new OaiHarvestException(
                  requestInfo = last.info,
                  url = endpoint,
                  stage = "resumption_token_parse",
                  firstId = firstId,
                  lastId = lastId,
                  cause = e
                )
              case Success(tokenResult) =>
                tokenResult match {
                  case None => onDeck = None
                  case Some((tokenValue, cursor, completeListSize)) =>
                    val requestInfo = last.info.copy(
                      resumptionToken = Some(tokenValue),
                      timestamp = System.currentTimeMillis(),
                      cursor = cursor,
                      completeListSize = completeListSize
                    )
                    val url = buildUrl(Some(tokenValue))
                    harvestLogger.logPageRequest(requestInfo, url.toString)
                    val page = getSinglePage(url, requestInfo)
                    page match {
                      case Right(_) =>
                        harvestLogger.logPageSuccess(requestInfo, url.toString)
                      case Left(err) =>
                        harvestLogger.logPageFailure(
                          requestInfo,
                          url.toString,
                          err.toString
                        )
                    }
                    onDeck = handleErrors(page, url, requestInfo)
                }
            }
            last
        }
      }
    }

  /** Get a single-page, un-parsed String response from the OAI feed, or an
    * error if one occurs.
    *
    * The page may contain an OAI error (HTTP code 200) or invalid XML. Detects
    * OAI protocol errors in full OAI-PMH documents (not only when the entire
    * response is a single <error> element).
    *
    * @param url
    *   URL for a single OAI request
    * @return
    *   Left(OaiError) when the response is valid XML with a top-level <error>,
    *   Right(OaiPage) otherwise.
    */
  def getSinglePage(
      url: URL,
      requestInfo: OaiRequestInfo
  ): Either[OaiError, OaiPage] = {
    if (sleep > 0)
      Thread.sleep(sleep)
    logger.info("Loading page {}: {}", url.toString, requestInfo)
    val page = HttpUtils.makeGetRequest(url)
    parseResponseBody(page, requestInfo)
  }

  /** Parse a response body string into Either OaiError or OaiPage. Detects full
    * OAI-PMH error documents (top-level <error> element). Used by getSinglePage
    * and by tests.
    */
  def parseResponseBody(
      page: String,
      requestInfo: OaiRequestInfo
  ): Either[OaiError, OaiPage] = {
    if (!page.contains("<error")) {
      Right(OaiPage(page, requestInfo))
    } else {
      Try(OaiXmlParser.parsePageIntoXml(OaiPage(page, requestInfo))) match {
        case Success(xml) =>
          val errorNode = xml \ "error"
          if (errorNode.nonEmpty) {
            val errorCode = errorNode \@ "code"
            Left(OaiError.errorForCode(errorCode, requestInfo))
          } else {
            Right(OaiPage(page, requestInfo))
          }
        case Failure(_) =>
          Right(OaiPage(page, requestInfo))
      }
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
