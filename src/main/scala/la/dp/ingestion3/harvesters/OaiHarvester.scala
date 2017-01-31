package la.dp.ingestion3.harvesters

import java.io.File
import java.net.URL

import la.dp.ingestion3.utils.FileIO
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.LogManager

import scala.xml.{NodeSeq, XML}
import scala.util.control.NonFatal
/**
  * OAI-PMH harvester for aggregating metadata into the DPLA
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
  private[this] val logger = LogManager.getLogger("OAI harvester")

  /**
    * Control method for harvesting
    *
    * @param verb String
    *             The OAI verb to use in the request
    */
  @throws(classOf[Exception])
  def runHarvest(verb: String): Unit = {
    // mutable resumption token for pagination
    var resumptionToken: String = ""

    // TODO FP-this, but I'm not sure if it can be
    try {
      do {
        val url = buildQueryUrl(resumptionToken, verb)
        val xml = getXmlResponse(url)
        // Get and check the error code if it exists
        val errorCode = getOaiErrorCode(xml)
        checkOaiErrorCode(errorCode)
        // Transform the XML response into a Map[File,String] and write to disk
        val docMap: Map[File, String] = getHarvestedRecords(xml)
        FileIO.writeFiles(docMap)
        // Get the new resumptionToken, empty String if at end
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
    * Takes the XML response from a ListRecords request and processes it
    * to create a Map[File, String] of target file locations and XML to save.
    *
    * @param xml NodeSeq
    *            Complete OAI-PMH XML response
    *
    * @return Map[File, String
    *         Map of the records in the response keyed to the fully qualified
    *         serialization path
    */
  def getHarvestedRecords(xml: NodeSeq): Map[File, String] = {
    (xml \\ "OAI-PMH" \\ "record").par
      .map( doc => {
        val provIdentifier = (doc \\ "header" \\ "identifier").text
        val dplaIdentifier = Harvester.generateMd5(provIdentifier)
        val outFile = new File(outDir, dplaIdentifier + ".xml")
        // pushes the key-value pair onto the ParSeq trait
        outFile -> doc.text
      }
    ).toList.toMap[File, String]
    // todo I want to unpack why .toList is required here
  }

  /**
    * Get the resumptionToken from the response
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
   def getResumptionToken(xml: NodeSeq): String = {
     (xml \\ "OAI-PMH" \\ "resumptionToken").text
   }

  /**
    * Get the error property if it exists
    *
    * @return String
    *         The error code if it exists otherwise empty string
    */
  def getOaiErrorCode(xml: NodeSeq): String = {
    (xml \\ "OAI-PMH" \\ "error").text
  }

  /**
    * Executes the request and returns the response
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
      case NonFatal(e) => {
        val msg: String = s"Unable to load response from " +
          s"OAI request:\n\t${url.toString}\n\t"
        throw HarvesterException(msg + e.getMessage )
      }
    }
  }

  /**
    * Builds an OAI request
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

    // If resumptionToken is empty then use the metadataPrefix parameter
    resumptionToken match {
      case "" => urlParams.setParameter("metadataPrefix", metadataPrefix)
      case _  => urlParams.setParameter("resumptionToken", resumptionToken)
    }
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
      case "" => logger.info("No error in OAI response.")
      case _ => {
        // TODO Need to figure out how to do a better check on error codes that isn't listing them
        throw HarvesterException(s"Error returned from OAI request.\nError code:${errorCode}")
      }
    }
  }
}
