package dpla.ingestion3.utils

import java.net.URL
import org.apache.logging.log4j.LogManager
import java.net.http.{HttpClient, HttpRequest}
import java.net.http.HttpClient.Redirect
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration
import scala.util.{Failure, Success, Try}

/** Utility object for making and working with HTTP requests. Uses the OkHttp
  * library.
  */
object HttpUtils {

  private val TIMEOUT = 20
  private val RETRIES = 5
  private val INITIAL_SLEEP = 1000

  private val httpClient = HttpClient
    .newBuilder()
    .followRedirects(Redirect.NORMAL)
    .connectTimeout(Duration.ofSeconds(TIMEOUT))
    .build()

  private val logger = LogManager.getLogger(getClass)

  /** Performs a Get request with retries and exponential backoff
    *
    * @param url
    *   URL to request
    * @return
    *   Body of the response
    */
  def makeGetRequest(
      url: URL,
      headers: Option[Map[String, String]] = None
  ): String =
    retry(RETRIES, INITIAL_SLEEP)(
      execute(constructRequest(url, headers))
    ) match {
      case Success(response) => response
      case Failure(e) =>
        logger.error(s"Failed to get response from $url", e)
        throw e
    }

  /** Give me a ping Vasili. One ping only.
    *
    * @param url
    *   URL to ping
    * @return
    */
  def validateUrl(url: String): Boolean =
    Try { new URL(url) } match {
      case Success(_) => true
      case Failure(_) => false
    }

  /** Executes the Request
    *
    * @param request
    *   Request to perform
    * @return
    *   Body of the response or error message
    */
  private def execute(request: HttpRequest): String = {
    val response = httpClient.send(request, BodyHandlers.ofString())
    if (response.statusCode() == 200) {
      response.body()
    } else {
      val msg = s"Unsuccessful request: ${request.uri().toString}\n" +
        s"Code: ${response.statusCode()}\n" +
        s"Message: ${response.body()}\n"
      throw new RuntimeException(msg)
    }
  }

  /** Generic retry method. Taken from stackoverflow...
    *
    * @param n
    *   Number of retries
    * @param wait
    *   Time to wait between retries
    * @param fn
    *   The method to retry
    * @tparam T
    *  Return type of the method
    * @return
    */
  @annotation.tailrec
  private def retry[T](n: Int, wait: Long)(fn: => T): Try[T] =
    util.Try { fn } match {
      case x: Success[T] => x
      // If we haven't maxed out our retries
      case _ if n > 1 =>
        logger.warn("Retry " + n)
        Thread.sleep(wait)
        // TODO some better way of incrementing wait period
        retry(n - 1, wait * 2)(fn)

      // Retries exhausted, return whatever the last result was
      case fn => fn
    }

  /** Constructs a Request object from the URL and headers
    *
    * @param url
    *   URL
    * @param headers
    *   HTTP headers
    * @return
    *   Request object
    */
  private def constructRequest(
      url: URL,
      headers: Option[Map[String, String]] = None
  ): HttpRequest = {
    val request = HttpRequest
      .newBuilder()
      .uri(url.toURI)

    headers match {
      case Some(h) =>
        h.foreach({ case (key, value) => request.header(key, value) })
      case _ => // Do nothing, no headers
    }

    request.build()
  }
}
