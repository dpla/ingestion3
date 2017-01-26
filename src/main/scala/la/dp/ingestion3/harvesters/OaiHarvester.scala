package la.dp.ingestion3.harvesters

import java.io.File

import la.dp.ingestion3.utils.{FileIO, Utils}
import org.apache.http.client.utils.URIBuilder

import scala.xml._

trait OaiHarvester {

  /**
    * Harvest all records from the OAI-PMH endpoint
    *
    * @param outDir @param resumptionTokenStr
    * @param endpoint
    * @param metadataPrefix
    * @param verb
    * @return Resumption token from the last request
    */
  def harvest(outDir: File,
              resumptionTokenStr: String = "",
              endpoint: String,
              metadataPrefix: String,
              verb: String): String = {
    // Build the URL
    val urlParams = new URIBuilder()
      .setScheme("http")
      .setHost(endpoint)
      .setParameter("verb", verb)

    // This could be a bit more elegant but basically if your query uses
    // a resumption token than passing the metadataPrefix is unneeded
    if (resumptionTokenStr.nonEmpty)
      urlParams.setParameter("resumptionToken", resumptionTokenStr)
    else
      urlParams.setParameter("metadataPrefix", metadataPrefix)

    val url = urlParams.build.toURL

    // Load XML from constructed URL
    val xml = XML.load(url)

    // Get the resumption token
    val resumptionTokenNode = xml \\ "OAI-PMH" \\ verb \\ "resumptionToken"

    // Get all docs in response
    val docs = xml \\ "OAI-PMH" \\ verb \\ "record"

    // Needs an FP rewrite...
    for (xmlDoc <- docs) {
      val provIdentifier = xmlDoc \\ "header" \\ "identifier"
      val dplaIdentifier = Harvester.generateMd5(provIdentifier.text)
      val originalRecordFilename = List(dplaIdentifier, ".xml").mkString("")
      val outFile = new File(outDir, originalRecordFilename)
      val prettyXml = Utils.formatXml(xmlDoc)

      // Serialize to disk
      FileIO.writeFile(prettyXml, outFile)
    }
    return resumptionTokenNode.text
  }
}







