package la.dp.ingestion3.harvesters

import java.io.{File, IOException}
import java.net.URL

import la.dp.ingestion3.utils.FileIO
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.LogManager

import scala.xml.{NodeSeq, SAXParseException, XML}

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

  // Logging object
  private[this] val logger = LogManager.getLogger("harvester")
  // Trying to store harvested data into a mutableMap and then seralize
  // to disk after harvest is done.

  /**
    * Control method for harvesting
    *
    * @param verb String
    *             The OAI verb to use in the request
    */
  @throws(classOf[Exception])
  def runHarvest(verb: String): Unit = {

    var resumptionToken: String = ""

    try {
      do {
        val url = buildQueryUrl(resumptionToken, verb)
        val xml = getXmlResponse(url)
        // Get and check the error code if it exists
        val errorCode = getErrorCode(xml)
        if (Predef.augmentString(errorCode).nonEmpty) {
          checkOaiErrorCode(errorCode)
        }
        // Transform the XML response into a Map and write Map that to disk
        val docMap = getHarvestedRecords(xml)
        FileIO.writeFiles(docMap)

        resumptionToken = getResumptionToken(xml)

      } while (Predef.augmentString(resumptionToken).nonEmpty)
    } catch {
      // TODO improve this error messaging
      case harvestError: HarvesterException => {
        logger.error(harvestError.getMessage)
        throw new Exception("Harvest failed")
      }
    }
  }

  /**
    * Makes a single request to the OAI endpoint and adds the records
    * in the result to a Map[File, String] object
    * TODO: I think that the errorCode checking should bubble up from here but
    * TODO: trapping it is easier for now. This should be a point
    * TODO: of further discussion in terms of error handing across
    * TODO: the project (styles and practices)
    *
    * @param xml NodeSeq
    *            Complete OAI-PMH XML response
    *
    * @return Map[File, String
    *         Map of the records in the response keyed to the fully qualified
    *         serialization path
    */
  def getHarvestedRecords(xml: NodeSeq): Map[File, String] = {
    val docs: NodeSeq = xml \\ "OAI-PMH" \\ "record"
    //
    var harvestedRecords = scala.collection.mutable.Map[File, String]()
    // Attempt at FP, there is probably a cleaner way to write this with more tests
    docs.par
      .foreach { doc => {
        // TODO this path should not be fixed but should be a parameter
        val provIdentifier = (doc \\ "header" \\ "identifier").text
        val dplaIdentifier = Harvester.generateMd5(provIdentifier)
        val outFile = new File(outDir, dplaIdentifier + ".xml")
        harvestedRecords.put(outFile, doc.text)
      }
    }
    harvestedRecords.toMap
  }

  /**
    * Get the resumptionToken in the response
    *
    * @param xml NodeSeq
    *            The complete XML response
    * @return String
    *         The resumptionToken to fetch the next set of records
    *         or an empty string if no more records can be fetched. An
    *         empty string does not mean all records were successfully
    *         harvested (an error could have occured when fetching), only
    *         that there are no more records that can be fetched.
    */
   def getResumptionToken(xml: NodeSeq): String = (xml \\ "OAI-PMH" \\ "resumptionToken").text

  /**
    * Get the error property if it exists
    *
    * @return String
    *         The error code if it exists otherwise empty string
    */
  def getErrorCode(xml: NodeSeq): String = (xml \\ "OAI-PMH" \\ "error").text

  /**
    * Executes the request and returns the XML response
    * as a NodeSeq object
    *
    * @param url URL
    *            OAI request URL
    * @return NodeSeq
    *         XML response
    */
  @throws(classOf[HarvesterException])
  def getXmlResponse(url: URL): NodeSeq = {
    try {
      XML.load(url)
    } catch {
      // TODO more nuanced error handling and logging
      case e:SAXParseException => {
        val msg = s"Unable to parse XML from response\n\t ${url.toString}\n\t"
        throw HarvesterException(msg + e.getMessage)
      }
      case e:IOException => {
        val msg = "Unable to load response from OAI request\n\t"
        throw HarvesterException(msg + e.getMessage)
      }
      case e => {
        throw HarvesterException(e.getMessage)
      }
    }
  }

  /**
    * Builds a OAI request URL
    *
    * @param resumptionToken String
    *                        Optional, token used to resume a previous harvest request
    * @param verb String
    *             OAI request verb
    *             See https://www.openarchives.org/OAI/openarchivesprotocol.html#ProtocolMessages
    * @return URL
    */
  def buildQueryUrl(resumptionToken: String = "",
                    verb: String): URL = {

    // Build the URL parameters
    val urlParams = new URIBuilder()
      .setScheme(endpoint.getProtocol)
      .setHost(endpoint.getHost)
      .setPath(endpoint.getPath)
      .setParameter("verb", verb)

    // TODO there is probably a much more elegant way to write this if
    if (resumptionToken.isEmpty)
      urlParams.setParameter("metadataPrefix", metadataPrefix)
    else
      urlParams.setParameter("resumptionToken", resumptionToken)

    urlParams.build.toURL
  }

  /**
    * Checks the error response codes in the OAI response
    *
    * @param errorCode String
    *                  The error code from the OAI response
    *                  For expected values see:
    *                   https://www.openarchives.org/OAI/openarchivesprotocol.html#ErrorConditions
    * @throws HarvesterException If an error code that should terminate the harvest prematurely is
    *                            received
    */
  @throws(classOf[HarvesterException])
  def checkOaiErrorCode(errorCode: String): Unit = {
    errorCode match {
      case "" => logger.info("No error in OAI request")
      case _ => {
        // Need to figure out how to do a better check on error codes that isn't listing them
        throw HarvesterException(s"Error returned from OAI request.\nError code:${errorCode}")
      }
    }
  }
}
