package la.dp.ingestion3.harvesters

import java.io.File
import java.net.URL

import la.dp.ingestion3.utils.FileIO
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.LogManager
import scala.collection.mutable

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
                    outDir: File,
                    idPath: String = "" ) {

  // Logging object
  private[this] val logger = LogManager.getLogger("harvester")
  // Trying to store harvested data into a mutableMap and then seralize
  // to disk after harvest is done.
  private val harvestedRecords: mutable.Map[File, String] = mutable.Map[File, String]()

  /**
    * Control method for harvesting
    *
    * @param verb String
    *             The OAI verb to use in the request
    */
  def runHarvest(verb: String): Unit = {
    var resumptionToken: String = ""

    try {
      do {
        val url = buildQueryUrl(resumptionToken, verb)
        val xml = getXmlResponse(url)
        // Get and check the
        val errorCode = getErrorCode(xml)
        if (errorCode.nonEmpty) {
          checkOaiErrorCode(errorCode)
        }

        getHarvestedRecords(xml)
        resumptionToken = getResumptionToken(xml)

      } while (Predef.augmentString(resumptionToken).nonEmpty)
    } catch {
      // TODO improve this error messaging
      case he: HarvesterException => {
        val errMsg = s"Error during harvest.\nException message: ${he.getMessage}"
        logger.error(errMsg)
      }
    }

    // Serialize the map of records and file paths to disk (or wherever)
    FileIO.writeFiles(harvestedRecords.toMap)
  }

  /**
    * Makes a single request to the OAI endpoint and adds the records
    * in the result to a Map[File, String] object
    *
    * @param xml NodeSeq
    *            Complete OAI-PMH XML response
    *
    *          TODO: I think that the errorCode checking should bubble up from here but
    *          TODO: trapping it is easier for now. This should be a point
    *          TODO: of further discussion in terms of error handing across
    *          TODO: the project (styles and practices)
    *
    *          TODO: This method has a side-effect of mutating the class Map
    *          TODO: harvestRecords. It should return a Map of records from
    *          TODO: the request and then merge that map with an immutable map
    *          TODO: but I haven't figure that out yet
    */
   private def getHarvestedRecords(xml: NodeSeq): Unit = {
      val docs: NodeSeq = xml \\ "OAI-PMH" \\ "record"

      // Attempt at FP
      docs.par
          .foreach { doc => {
            // TODO this path should not be fixed
            val provIdentifier = (doc \\ "header" \\ "identifier").text
            val dplaIdentifier = Harvester.generateMd5(provIdentifier)
            val outFile = new File(outDir, dplaIdentifier + ".xml")

            // #harvest() probably shouldn't be writing these files to disk
            // So I moved serialization to its own method and store the harvested
            // records in a Map here. This approach may have scaling issues and
            // requires testing

            // FileIO.writeFile(doc.text, outFile)
            harvestedRecords.put(outFile, doc.text)
          }
        }
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
    * TODO clarify error handling. Was the try/catch
    * TODO block even necessary here?
    *
    * @param url URL
    *            OAI request URL
    * @return NodeSeq
    *         XML response
    */
  def getXmlResponse(url: URL): NodeSeq = XML.load(url)

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
      .setParameter("verb", verb)

    // TODO there is probably a much more elegant way to write this if
    if (resumptionToken.isEmpty)
      urlParams.setParameter("metadataPrefix", metadataPrefix)
    else
      urlParams.setParameter("resumptionToken", resumptionToken)

    // Build and return the URL
    urlParams.build.toURL
  }

  /**
    * Checks the error response codes in the OAI response
    *
    * @param errorCode String
    *                  The error code from the OAI response
    *                  See https://www.openarchives.org/OAI/openarchivesprotocol.html#ErrorConditions
    * @throws HarvesterException if an error code that should terminate the harvest prematurely is
    *                            received
    */
  @throws(classOf[HarvesterException])
  def checkOaiErrorCode(errorCode: String): Unit = {
    // This is not my preferred formatting style but it is much more readable
    errorCode match {
      case "badArgument"             => throw HarvesterException("Bad argument in OAI request.")
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
