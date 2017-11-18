package dpla.ingestion3.harvesters.oai.refactor

import java.net.URL

import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class OaiMultiPageResponseBuilder(endpoint: String,
                                  verb: String,
                                  metadataPrefix: Option[String] = None,
                                  set: Option[String] = None)
  extends Serializable {

  /**
    * Main entry point.
    * Get all pages of results from an OAI feed.
    * OaiPages may contain an OAI error (HTTP code 200) or invalid XML.
    *
    * Makes an initial call to the feed to get the first page of results.
    * For this and all subsequent pages, calls the next page if a resumption
    * token is present.
    *
    * @return Un-parsed response page from OAI requests, including OaiPages
    *         and OaiErrors.
    */
  def getResponse: List[Either[OaiError, OaiPage]] = {

    @tailrec
    def loop(data: List[Either[OaiError, OaiPage]]):
      List[Either[OaiError, OaiPage]] = {

      data.headOption match {
        case Some(pageEither) =>
          getResumptionToken(pageEither) match {
            // If the previous response did not contain a resumption token,
            // return everything harvested up to this point.
            case None => data
            // Otherwise, get the next page response.
            case Some(token) =>
              val url: Try[URL] = buildUrl(Some(token))
              val nextResponse: Either[OaiError, OaiPage] = getSinglePage(url)
              loop(nextResponse :: data)
          }
        // If data is empty, return it.
        // This is only reached if something really strange happened.
        case _ => data
      }
    }

    val url: Try[URL] = buildUrl()
    val firstResponse: Either[OaiError, OaiPage] = getSinglePage(url)
    loop(List(firstResponse))
  }

  /**
    * Get a single-page, un-parsed String response from the OAI feed, or an error
    * if one occurs.
    *
    * The page may contain an OAI error (HTTP code 200) or invalid XML.
    *
    * @param urlTry Try[URL] URL for a single OAI request
    * @return Either[OaiError, Page]
    */
  def getSinglePage(urlTry: Try[URL]): Either[OaiError, OaiPage] = {
    urlTry match {
      // Error building URL
      case Failure(e) => Left(OaiError(e.toString))
      case Success(url) => {
        HttpUtils.makeGetRequest(url) match {
          // HTTP error
          case Failure(e) => Left(OaiError(e.toString, Some(url.toString)))
          case Success(page) => Right(OaiPage(page))
        }
      }
    }
  }

  /**
    * Builds an OAI request.
    * Endpoint and verb must be present in all requests, including those
    * containing a resumptionToken.
    *
    * Set and metadataPrefix can only be present in a first request, and
    * cannot be present in any request that contains a resumptionToken.
    *
    * @see https://www.openarchives.org/OAI/openarchivesprotocol.html#ProtocolMessages
    *
    * @param resumptionToken An OAI resumption token
    * @return Try[URL]
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

  /**
    * Get the resumptionToken from the response
    *
    * @param pageEither Either[OaiError, OaiPage]
    *                   OaiError a previously incurred error.
    *                   OaiPage a single page OAI response.
    *
    * @return Option[String]
    *         The resumptionToken to fetch the next page response.
    *         or None if no more records can be fetched.
    *         No resumption token does not necessarily mean that all pages
    *         were successfully fetched (an error could have occurred),
    *         only that no more pages can be fetched.
    */
  def getResumptionToken(pageEither: Either[OaiError, OaiPage]):
    Option[String] = pageEither match {

    // If the pageEither is an error, return None.
    case Left(error) => None
    // Otherwise, attempt to parse a resumption token.
    // Use a regex String match to find the resumption token b/c it is too
    // costly to map the String to XML at this point.
    case Right(oaiPage) =>
      val pattern = """<resumptionToken.*>(.*)</resumptionToken>""".r
      pattern.findFirstMatchIn(oaiPage.page) match {
        case Some(m) => Some(m.group(1))
        case _ => None
      }
  }
}
