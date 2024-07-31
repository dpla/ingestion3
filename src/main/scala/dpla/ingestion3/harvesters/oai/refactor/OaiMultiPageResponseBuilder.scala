package dpla.ingestion3.harvesters.oai.refactor

import java.net.URL

import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder

import scala.util.{Failure, Success, Try}

class OaiMultiPageResponseBuilder(
    endpoint: String,
    verb: String,
    metadataPrefix: Option[String] = None,
    set: Option[String] = None,
    sleep: Int = 0
) extends Serializable {

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
  def getResponse: Iterable[Either[OaiError, OaiPage]] =
    new Iterable[Either[OaiError, OaiPage]] {
      override def iterator = new Iterator[Either[OaiError, OaiPage]]() {

        var onDeck: Option[Either[OaiError, OaiPage]] = Some(
          getSinglePage(buildUrl())
        )

        override def hasNext: Boolean = onDeck.isDefined

        override def next(): Either[OaiError, OaiPage] = onDeck match {
          case None => Left(OaiError("Called next() on end of iterator.", None))
          case Some(last) =>
            val response = last
            val resumptionToken = getResumptionToken(last)
            resumptionToken match {
              case None => onDeck = None
              case Some(_) =>
                onDeck = Some(getSinglePage(buildUrl(resumptionToken)))
            }
            response
        }
      }
    }

  /** Get a single-page, un-parsed String response from the OAI feed, or an
    * error if one occurs.
    *
    * The page may contain an OAI error (HTTP code 200) or invalid XML.
    *
    * @param urlTry
    *   Try[URL] URL for a single OAI request
    * @return
    *   Either[OaiError, Page]
    */
  def getSinglePage(urlTry: Try[URL]): Either[OaiError, OaiPage] = {
    urlTry match {
      // Error building URL
      case Failure(e) => Left(OaiError(e.toString))
      case Success(url) => {
        if (sleep > 0) {
          Thread.sleep(sleep)
        }
        HttpUtils.makeGetRequest(url) match {
          // HTTP error
          case Failure(e) => Left(OaiError(e.toString, Some(url.toString)))
          case Success(page) =>
            val pattern = """<error.*>(.*)</error>""".r
            pattern.findFirstMatchIn(page) match {
              case Some(m) => Left(OaiError(m.group(1), Some(url.toString)))
              case _       => Right(OaiPage(page))
            }
        }
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
  def buildUrl(resumptionToken: Option[String] = None): Try[URL] = Try {

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

  /** Get the resumptionToken from the response
    *
    * @param pageEither
    *   Either[OaiError, OaiPage] OaiError a previously incurred error. OaiPage
    *   a single page OAI response.
    * @return
    *   Option[String] The resumptionToken to fetch the next page response. or
    *   None if no more records can be fetched. No resumption token does not
    *   necessarily mean that all pages were successfully fetched (an error
    *   could have occurred), only that no more pages can be fetched.
    */
  def getResumptionToken(
      pageEither: Either[OaiError, OaiPage]
  ): Option[String] = pageEither match {

    // If the pageEither is an error, return None.
    case Left(error) => None
    // Otherwise, attempt to parse a resumption token.
    // Use a regex String match to find the resumption token b/c it is too
    // costly to map the String to XML at this point.
    case Right(oaiPage) =>
      val pattern = """<resumptionToken.*>(.*)</resumptionToken>""".r
      pattern.findFirstMatchIn(oaiPage.page) match {
        case Some(m) =>
          val rt = m.group(1)
          if (rt.isEmpty)
            None
          else
            Some(rt.trim)
        case _ => None
      }
  }
}
