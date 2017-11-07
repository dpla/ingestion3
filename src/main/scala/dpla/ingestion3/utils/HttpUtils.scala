package dpla.ingestion3.utils

import java.net.URL
import java.util.concurrent.TimeUnit

import okhttp3.{OkHttpClient, Request}

import scala.util.{Success, Try}

/**
  * Utility object for making and working with HTTP requests. Uses the OkHttp library.
  *
  */
object HttpUtils {

  protected lazy val timeout = 5 // TODO load from config?
  protected lazy val retryMax = 5
  protected lazy val backoffMax = 500

  protected lazy val httpClient: OkHttpClient = new OkHttpClient.Builder()
    .connectTimeout(timeout, TimeUnit.SECONDS)
    .retryOnConnectionFailure(true)
    .followRedirects(true)
    .build()

  /**
    * Performs a Get request with retries and exponential backoff
    *
    * @param url URL to request
    * @return Body of the response
    */
  def makeGetRequest(url: URL, headers: Option[Map[String,String]] = None): Try[String] = {
    retry(retryMax, backoffMax)( execute(constructRequest(url, headers)) )
  }

  /**
    * Give me a ping Vasili. One ping only.
    *
    * @param url URL to ping
    * @return
    */
  def validateUrl(url: String): Boolean = {
    httpClient
      .newCall(constructRequest(new URL(url)))
      .execute
      .isSuccessful
  }

  /**
    * Executes the Request
    *
    * @param request Request to perform
    * @return Body of the response or error message
    */
  private def execute(request: Request): String = {
    val response = httpClient
      .newCall(request)
      .execute

    // try is used here to ensure closure of response
    try {
      if (response.isSuccessful) {
        response.body().string()
      } else {
        val msg = s"Unsuccessful request: ${request.url().toString}\n" +
          s"Code: ${response.code()}\n" +
          s"Message: ${response.message()}\n"
        throw new RuntimeException(msg)
      }
    } finally {
      response.close()
    }
  }

  /**
    * Generic retry method. Taken from stackoverflow...
    *
    * @param n Number of retries
    * @param wait Time to wait between retries
    * @param fn The method to retry
    * @tparam T
    * @return
    */
  @annotation.tailrec
  private def retry[T](n: Int, wait: Long)(fn: => T): Try[T] = {
    util.Try { fn } match {
      case x: Success[T] => x
      // If we haven't maxed out our retries
      case _ if n > 1 => {
        Thread.sleep(wait)
        // TODO some better way of incrementing wait period
        retry(n - 1, wait*2)(fn)
      }
      // Retries exhausted, return whatever the last result was
      case fn => fn
    }
  }

  /**
    * Constructs a Request object from the URL and headers
    *
    * @param url URL
    * @param headers HTTP headers
    * @return Request object
    */
  private def constructRequest(url: URL, headers: Option[Map[String,String]] = None ): Request = {
    val request = new Request
      .Builder()
      .url(url)

    headers match {
      case Some(h) => h.foreach({ case (key, value) => request.addHeader(key, value) })
      case _ => // Do nothing, no headers
    }

    request.build()
  }
}
