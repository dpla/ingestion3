package la.dp.ingestion3.harvesters

import java.io.File
import java.net.URL

import la.dp.ingestion3.utils.FileIO
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.LogManager

import scala.xml.{NodeSeq, XML}

/**
  * OAI-PMHester for aggregating metadata into the DPLA
  *
  * @param endpoint URL
  *                 Address of the OAI endpoint
  * @param metadataPrefix String
  *                       Metadata prefix to harvest
  * @param outDir File
  *               Location to save the harvested records
  */
class OaiHarvester (endpoint: URL,
                    metadataPrefix: String,
                    outDir: File) {

  private[this] val logger = LogManager.getLogger("harvester")

  /**
    * Harvest all records from the OAI-PMH endpoint and saves
    * the records as individual XML files
    *
    * @param resumptionToken String
    *                        Optional token to resume a previous harvest
    * @param verb String
    *             OAI verb, ListSets, ListRecords
    * @return Resumption token from the last request
    */
  def harvest(resumptionToken: String = "",
              verb: String): String = {

    // Build the URL
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
    // TODO Network error handling(?)
    val xml = getResponse(url)

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
          FileIO.writeFile(doc.text, outFile)
        }
      }
    // Return the resumptionToken or empty string if it doesn't exist
    getResumptionToken(xml)
  }

  /**
    * Get the resumptionToken in the response
    *
    * @param xml NodeSeq
    *            The XML response
    * @return String
    *         The value in resumptionToken property, if the property does not exist than
    *         and empty string is returned
    */
  def getResumptionToken(xml: NodeSeq): String = {
    (xml \\ "OAI-PMH" \\ "ListRecords" \\ "resumptionToken").text
  }

  /**
    * Executes the request and returns the response
    * TODO improve error handling
    *
    * @param url URL
    *            OAI request URL
    * @return NodeSeq
    *         XML response
    * @throws HarvesterException If unable to get a response
    */
  @throws(classOf[HarvesterException])
  def getResponse(url: URL): NodeSeq = {
    try {
      XML.load(url)
    } catch {
      case e: Exception => {
        logger.error(e.getMessage)
        throw HarvesterException(s"Unable to get response from ${url.toString}\n\t${e.getMessage}")
      }
    }
  }

  /**
    * Checks the error response codes
    *
    * @param errorCode String
    *                  The error code from the OAI response
    *                  See https://www.openarchives.org/OAI/openarchivesprotocol.html#ErrorConditions
    * @throws HarvesterException If there was a valid error code
    * @throws Exception If there was an unknown error code
    */
  @throws(classOf[HarvesterException, Exception])
  def checkOaiErrorCode(errorCode: String): Unit = {
    // This is not my perferred style but it is much more readable
    errorCode match {
      case "badArguement"             => throw HarvesterException("BadArguement in OAI request.")
      case "badResumptionToken"       => throw HarvesterException("Bad resumption token in OAI request")
      case "badVerb"                  => throw HarvesterException("BadVerb in harvest request")
      case "idDoesNotExist"           => throw HarvesterException("Item does not exist in feed.")
      case "noMetadataFormats"        => throw HarvesterException("No metadata formats available")
      case "noSetHierarchy"           => throw HarvesterException("Sets not supported")
      case "cannotDisseminateFormat"  => throw HarvesterException("Correct the metadataPrefix and restart")
      case "noRecordsMatch"           => logger.warn("No records returned from request")
      case ""                         => logger.info("No error")
      case _                          => throw Exception(s"Unknown error code: ${errorCode}")
    }
  }
}
