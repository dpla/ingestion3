package la.dp.ingestion3.harvesters

import java.io.File

import la.dp.ingestion3.utils.FileIO
import org.apache.http.client.utils.URIBuilder
import org.apache.jena.sparql.function.library.leviathan.root

import scala.xml.{NodeSeq, XML}

/**
  *
  * @param endpoint
  * @param metadataPrefix
  * @param outDir
  * @param scheme
  */
class OaiHarvester (endpoint: String,
                   metadataPrefix: String,
                   outDir: File,
                   scheme: String = "http") {

  /**
    * Harvest all records from the OAI-PMH endpoint and saves
    * the records as individual XML files
    *
    * @param resumptionToken Optional token to resume a previous harvest
    * @param verb OAI verb, ListSets, ListRecords
    * @return Resumption token from the last request
    */
  def harvest(resumptionToken: String = "",
              verb: String): String = {

    // Build the URL
    val urlParams = new URIBuilder()
      .setScheme(scheme)
      .setHost(endpoint)
      .setParameter("verb", verb)

    // This could be a bit more elegant but if resumptionToken is provided
    // then metadataPrefix is an invalid parameter
    if (resumptionToken.isEmpty)
      urlParams.setParameter("metadataPrefix", metadataPrefix)
    else
      urlParams.setParameter("resumptionToken", resumptionToken)

    // Build the URL
    val url = urlParams.build.toURL

    // Load XML from constructed URL
    // TODO Network error handling(?)
    val xml = XML.load(url)

    // Check for and handle errors in response
    // TODO Response for each error code
    (xml \\ "error" \ "@code").text match {
      case "cannotDisseminateFormat" => "Format troubles"
      case "badResumptionToken" => "Resumption token expired or is invalaid" // throw error and abort
      case "noRecordsMatch" => "No records found" // No exception?
      case "noSetHierarchy" => "Hierarchy is a pain"
      case _ => "Alls well "
    }

    val docs:NodeSeq = xml \\ "OAI-PMH" \\ "ListRecords" \\ "record"

    // Attempt at FP
    docs.par
      .map(doc => {
        val provIdentifier = (doc \\ "header" \\ "identifier").text
        val dplaIdentifier = Harvester.generateMd5(provIdentifier)
        val filename = dplaIdentifier+".xml"
        val outFile = new File(outDir, filename)

        // TODO this might go someplace else...
        FileIO.writeFile(doc.text, outFile)
      })

    return (xml \\ "OAI-PMH" \\ verb \\ "resumptionToken").text
  }
}
