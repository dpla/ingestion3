import dpla.ingestion3.harvesters.ResourceSyncUrlBuilder
import dpla.ingestion3.utils.Utils

import scalaj.http.Http
import org.apache.commons.httpclient._
import org.apache.http.client.methods.CloseableHttpResponse

val rsUrlBuilder = new ResourceSyncUrlBuilder()
val params = Map("Accept" -> "application/xml,text/turtle")
val itemFetchParams = Utils.updateParams(List(params,
  Map("endpoint" -> "http://hyphy.demo.hydrainabox.org/concern/generic_works/kw741ky5674")))
val item = rsUrlBuilder.buildQueryUrl(itemFetchParams)
val request = Http(item.toString)

val ttlRqst = request.header("accept","text/turtle")
ttlRqst.headers




val rsp = ttlRqst.execute()

rsp.headers

/**
  *
  */

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

val httpclient = HttpClients.createDefault
val httpGet = new HttpGet("http://hyphy.demo.hydrainabox.org/concern/generic_works/kw741ky5674")

httpGet.addHeader("Accept","text/turtle")

val response1: CloseableHttpResponse = httpclient.execute(httpGet)

try {

  System.out.println(response1.getStatusLine)
  val entity1 = response1.getEntity
  // do something useful with the response body
  // and ensure it is fully consumed
   val body = EntityUtils.toString(entity1)

  body
} finally {
  response1.close()
}
