package la.dp.ingestion3.harvesters

import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.Text
import org.apache.http.client.utils.URIBuilder

import scala.xml._

trait OaiHarvester {

  /**
    * Harvest all records from the OAI-PMH endpoint
    *
    * @param writer
    * @param resumptionTokenStr
    * @param endpoint
    * @param metadataPrefix
    * @param verb
    * @return Resumption token from the last request
    */
  def harvest(writer: Writer,
              resumptionTokenStr: String = "",
              endpoint: String,
              metadataPrefix: String,
              verb: String): String = {
    // Build the URL
    val urlParams = new URIBuilder()
      .setScheme("http")
      .setHost(endpoint)
      .setParameter("verb", verb)

    if (resumptionTokenStr.nonEmpty)
      urlParams.setParameter("resumptionToken", resumptionTokenStr)
    else
      urlParams.setParameter("metadataPrefix", metadataPrefix)

    val x = urlParams.getQueryParams
    val iter = x.iterator()
    while (iter.hasNext) {
      val y = iter.next()
      println(y)
    }
    val url = urlParams.build.toURL

    // Load XML from constructed URL
    val xml = XML.load(url)

    val resumptionTokenNode = xml \\ "OAI-PMH" \\ verb \\ "resumptionToken"
    val docs = xml \\ "OAI-PMH" \\ verb \\ "record"

    for (doc <- docs) {
      val provIdentifier = doc \\ "header" \\ "identifier"
      val dplaIdentifier = Harvester.generateMd5(provIdentifier.text)
      writer.append(new Text(dplaIdentifier), new Text(doc.toString))
    }
    return resumptionTokenNode.text
  }
}







