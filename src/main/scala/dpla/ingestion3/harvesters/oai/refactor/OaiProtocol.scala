package dpla.ingestion3.harvesters.oai.refactor

import java.net.URL

import dpla.ingestion3.harvesters.UrlBuilder
import dpla.ingestion3.utils.HttpUtils
import org.apache.http.client.utils.URIBuilder

import scala.annotation.tailrec
import scala.collection.TraversableOnce
import scala.util.{Failure, Success, Try}

class OaiProtocol(oaiConfiguration: OaiConfiguration) extends OaiMethods with UrlBuilder {

  override def listAllRecordPages:
    TraversableOnce[Either[OaiPage, OaiError]] = {

    val metadataPrefix = oaiConfiguration.metadataPrefix
    val endpoint = oaiConfiguration.endpoint

    val baseParams = Map("endpoint" -> endpoint, "verb" -> "ListRecords")
    val multiPageResponse = getMultiPageResponse(baseParams)
    // TODO: This is a convenient but probably not very useful way to make the
    // return type a TraversableOnce until I figure out a better way.
    multiPageResponse.toIterator
  }

  override def listAllRecordPagesForSet(setEither: Either[OaiSet, OaiError]):
    TraversableOnce[Either[OaiRecord, OaiError]] = ???

  override def parsePageIntoRecords(pageEither: Either[OaiPage, OaiError]):
    TraversableOnce[Either[OaiRecord, OaiError]] = ???

  override def listAllSets: TraversableOnce[Either[OaiSet, OaiError]] = ???

  /**
    * Get all pages of results from an OAI feed.
    * Pages may contain an OAI error (HTTP code 200) or invalid XML.
    *
    * Makes an initial call to the feed to get the first page of results.
    * For this and all subsequent pages, calls the next page if a resumption
    * token is present.
    *
    * @return Un-parsed response page from OAI requests, including OaiSources
    *         and OaiErrors.
    */
  def getMultiPageResponse(baseParams: Map[String, String], opts: Map[String, String] = Map()):
    List[Either[OaiPage, OaiError]] = {

    @tailrec
    def loop(data: List[Either[OaiPage, OaiError]]): List[Either[OaiPage, OaiError]] = {

      data.headOption match {
        // Stops the harvest if an OaiError was trapped and returns everything
        // harvested up that this point plus the error.
        case Some(Right(_)) => data
        // If it was a valid page response then extract data and call the next page.
        case Some(Left(previous)) =>
          val text = previous.page
          val token = OaiResponseProcessor.getResumptionToken(text)

          token match {
            // If the page does not contain a token, return everything harvested
            // up to this point.
            case None => data
            // Otherwise, get the next page.
            case Some(token) =>
              // Resumption tokens are exclusive, meaning a request with a token
              // cannot have any additional optional args.
              val nextParams = baseParams + ("resumptionToken" -> token)
              val nextResponse = getSinglePageResponse(nextParams)
              loop(nextResponse :: data)
          }
        // This is only reached if something really strange happened
        // If there is an error or unexpected response type, return all data
        // collected up to this point (including the error or unexpected response).
        case _ => data
      }
    }

    // The initial request must include all optional args.
    val firstParams = baseParams ++ opts
    val firstResponse = getSinglePageResponse(firstParams)
    loop(List(firstResponse))
  }

  /**
    * Get a single-page, unparsed response from the OAI feed, or an error if
    * one occurs.
    *
    * The page may contain an OAI error (HTTP code 200) or invalid XML.
    *
    * @param queryParams parameters for a single OAI request.
    * @return OaiPage or OaiError
    */
  def getSinglePageResponse(queryParams: Map[String, String]): Either[OaiPage, OaiError] = {
    getUrl(queryParams) match {
      // Error building URL
      case Right(error) => Right(error)
      case Left(url) => {
        HttpUtils.makeGetRequest(url) match {
          // HTTP error
          case Failure(e) => Right(OaiError(e.toString, Some(url.toString)))
          case Success(page) => Left(OaiPage(page))
        }
      }
    }
  }

  /**
    * Tries to build a URL from the parameters
    *
    * @param queryParams HTTP parameters
    * @return Either[URL, OaiError]
    */
  def getUrl(queryParams: Map[String, String]): Either[URL, OaiError] =
  Try { buildUrl(queryParams) } match {
    case Success(url) => Left(url)
    case Failure(e) =>
      val queryString = queryParams.map(_.productIterator.mkString(":")).mkString("|")
      val errorString = e.toString
      val msg = s"Failed to make URL with params $queryString.  $errorString"
      Right(OaiError(msg))
  }

  /**
    * Builds an OAI request
    *
    * @param params Map of parameters needed to construct the URL
    *               OAI request verbs
    *               See https://www.openarchives.org/OAI/openarchivesprotocol.html#ProtocolMessages
    * @return URL
    */
  override def buildUrl(params: Map[String, String]): URL = {
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

    // Set optional properties.
    resumptionToken.foreach(t => urlParams.setParameter("resumptionToken", t))
    set.foreach(s => urlParams.setParameter("set", s))
    metadataPrefix.foreach(prefix => urlParams.setParameter("metadataPrefix", prefix))

    urlParams.build.toURL
  }
}
