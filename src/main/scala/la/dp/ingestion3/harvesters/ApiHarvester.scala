
package la.dp.ingestion3.harvesters

import org.apache.http.HttpEntity
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.json4s.jackson.JsonMethods._
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.SequenceFile.Writer

trait ApiHarvester {
  
  /**
    * Prototype for API harvester
    *
    * @param endpoint
    *                 The API endpoint to harvest from
    * @param query
    * @param key
    * @param offset
    * @param fetchSize
    * @param outDir
    * @param writer
    * @return Int
   */
  def harvest(endpoint: String,
              query: String="*:*",
              key: String,
              offset: String="0",
              fetchSize: String="10",
              outDir: String,
              writer: Writer): Int = {

    var fetchAgain = false
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

    try {
      val entity: HttpEntity = response.getEntity()

      if (entity != null) {
        // Parse the JSON
        val json = parse(EntityUtils.toString(entity))
        // Select the docs array
        val docs = (json \\ "docs")
        // If the number of docs returned is less than the number
        // requested then there is no more to fetch
        fetchAgain = docs.children.length == fetchSize.toInt

        for (doc <- (json \\ "docs").children) {
          // Generate the DPLA id
          val identifier: String = Harvester.generateMd5((doc \ "id").toString)
          // Write to Hadoop Seq File
          writer.append(new Text(identifier), new Text(compact(doc)))
        }
      }
    } finally {
      response.close()
    }

    if (fetchAgain) {
      return offset.toInt + fetchSize.toInt
    }
    return -1
  }
}