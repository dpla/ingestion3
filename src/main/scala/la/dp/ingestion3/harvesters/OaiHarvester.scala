package la.dp.ingestion3.harvesters

import java.io.File
import java.net.URL

import la.dp.ingestion3.OaiHarvesterMain.printResults
import la.dp.ingestion3.utils.{FileIO, Utils}
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.LogManager

import scala.xml.{NodeSeq, XML}

/**
  * OAI-PMHester for aggregating metadata into the DPLA
  *
  * @param outDir File
  *               Location to save files
  *               TODO: this should be 100% agnostic S3, local, network
  * @param endpoint URL
  *                 The OAI endpoint to harvest against
  * @param metadataPrefix String
  *                       Metadata format of the response
  *                       https://www.openarchives.org/OAI/openarchivesprotocol.html#MetadataNamespaces
  */
class OaiHarvester (endpoint: URL,
                    metadataPrefix: String,
                    outDir: File) {

  private[this] val logger = LogManager.getLogger("harvester")

  /**
    * Control method for harvesting
    *
    * @param verb String
    *             The OAI verb to use in the request
    */
  def runHarvest(verb: String): Unit = {
    var resumptionToken: String = ""
    val start = System.currentTimeMillis()

    try {
      do {
        resumptionToken = harvest(resumptionToken, verb)
      } while (resumptionToken.nonEmpty)
      val end = System.currentTimeMillis()
      val recordsHarvested = Utils.countFiles(outDir, ".xml")
      val runtimeMs = end - start

      printResults(runtimeMs, recordsHarvested)

    } catch {
      case e: Exception => println(e.toString)
    }
  }

  /**
    * Harvest all records from the OAI-PMH endpoint and saves
    * the records as individual XML files
    *
    * @param resumptionToken String
    *                        Optional token to resume a previous harvest
    * @param verb String
    *             OAI verb, ListSets, ListRecords
    * @return  The resumptionToken to use to fetch the next set of records
    *          or an empty string if no more records can be fetched. An
    *          empty string does not mean all records were successfully
    *          harvested (an error could have occured when fetching), only
    *          that there are no more records that can be fetched.
    *
    *          TODO: I think that the error should bubble up from here but
    *          TODO: trapping it is easier for now. This should be a point
    *          TODO: of further discussion in terms of error handing across
    *          TODO: the project (styles and practices)
    */
   private def harvest(resumptionToken: String = "",
              verb: String): String = {

    // Build the URL parameters
    val urlParams = new URIBuilder()
      .setScheme(endpoint.getProtocol)
      .setHost(endpoint.getHost)
      .setParameter("verb", verb)
    if (resumptionToken.isEmpty)
      urlParams.setParameter("metadataPrefix", metadataPrefix)
    else
      urlParams.setParameter("resumptionToken", resumptionToken)

    // Build the URL
    val url = urlParams.build.toURL

    // Load XML from constructed URL
    val xml = getXmlResponse(url)

    // Check for and handle errors in response
    try {
      val errorCode = (xml \\ "error" \ "@code").text
      checkOaiErrorCode(errorCode)
    } catch {
      case he: HarvesterException => logger.error(s"Error code in response to: ${url.toString}\n\n" + he.getMessage)
    }

    val docs: NodeSeq = xml \\ "OAI-PMH" \\ "ListRecords" \\ "record"

    // Attempt at FP serialization
    docs.par
      .foreach {
        doc => {
          val provIdentifier = (doc \\ "header" \\ "identifier").text
          val dplaIdentifier = Harvester.generateMd5(provIdentifier)
          val outFile = new File(outDir, dplaIdentifier + ".xml")
          // TODO this might go someplace else...single responsibility principle...
          // TODO #harvest probably shouldn't be writing these files to disk
          FileIO.writeFile(doc.text, outFile)
        }
      }
    // Return
    getResumptionToken(xml)
  }

  /**
    * Get the resumptionToken in the response
    *
    * @param xml NodeSeq
    *            The complete XML response
    * @return String
    *         The value in resumptionToken property, if the property
    *         does not exist than an empty string is returned
    */
   def getResumptionToken(xml: NodeSeq): String = {
    (xml \\ "OAI-PMH" \\ "resumptionToken").text
  }

  /**
    * Executes the request and returns the response
    * TODO clarify error handling. Was the try/catch
    * TODO block even necessary here?
    *
    * @param url URL
    *            OAI request URL
    * @return NodeSeq
    *         XML response
    */
  def getXmlResponse(url: URL): NodeSeq = {
      XML.load(url)
  }

  /**
    * Checks the error response codes in the OAI response
    *
    * @param errorCode String
    *                  The error code from the OAI response
    *                  See https://www.openarchives.org/OAI/openarchivesprotocol.html#ErrorConditions
    * @throws HarvesterException
    */
  @throws(classOf[HarvesterException])
  def checkOaiErrorCode(errorCode: String): Unit = {
    // This is not my preferred formatting style but it is much more readable
    errorCode match {
      case "badArguement"             => throw HarvesterException("Bad arguement in OAI request.")
      case "badResumptionToken"       => throw HarvesterException("Bad resumption token in OAI request")
      case "badVerb"                  => throw HarvesterException("Bad verb in harvest request")
      case "idDoesNotExist"           => throw HarvesterException("Item does not exist in feed.")
      case "noMetadataFormats"        => throw HarvesterException("No metadata formats available")
      case "noSetHierarchy"           => throw HarvesterException("Sets not supported")
      case "cannotDisseminateFormat"  => throw HarvesterException("Incorrect metadata format")
      case "noRecordsMatch"           => logger.warn("No records returned from request")
      case ""                         => logger.info("No error")
      case _                          => throw HarvesterException(s"Unknown error code: ${errorCode}")
    }
  }
}
