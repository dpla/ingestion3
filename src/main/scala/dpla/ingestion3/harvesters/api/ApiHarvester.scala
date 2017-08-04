
package dpla.ingestion3.harvesters.api

import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.Text
import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.json4s.jackson.JsonMethods._

import scala.util.Try


trait ApiHarvester {

  /**
    * Prototype for API harvester
    *
    * @param endpoint
    * The API endpoint to harvest from
    * @param query
    * @param key
    * @param offset
    * @param fetchSize
    * @param outDir
    * @param writer
    * @return Int
    */
  def harvest(endpoint: String,
              query: String = "*:*",
              key: String,
              offset: String = "0",
              fetchSize: String = "10",
              outDir: String,
              writer: Writer): Int = {

    val httpclient = HttpClients.createDefault()
    // Construct the query URI
    val uri = new URIBuilder()
      .setScheme("https")
      .setHost(endpoint)
      .setPath("/solr/query")
      .setParameter("q", query)
      .setParameter("start", offset)
      .setParameter("rows", fetchSize)
      .build()

    val httpget = new HttpGet(uri)

    // Set the authentication token
    httpget.addHeader("X-Authentication-Token", key)
    // Execute the GET request

    val response: CloseableHttpResponse = httpclient.execute(httpget)

    //todo exception handling totally misses those from the last statement.

    val returnValue = Try {
      Option(response.getEntity) match {
        case None => -1 //no entity, no new offset
        case Some(entity) =>
          val json = parse(EntityUtils.toString(entity))
          val docs = json \\ "docs"
          for (doc <- docs.children)
            writer.append((doc \ "id").toString, new Text(compact(doc)))
          // Calculate the new offset value
          if (docs.children.length == fetchSize.toInt)
            offset.toInt + fetchSize.toInt
          else -1
      }
    }

    IOUtils.closeQuietly(response)
    returnValue.getOrElse(-1)
  }

}