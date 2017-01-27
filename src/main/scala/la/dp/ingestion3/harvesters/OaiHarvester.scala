package la.dp.ingestion3.harvesters

import java.io.File

import la.dp.ingestion3.utils.FileIO
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.LogManager

import scala.xml.{NodeSeq, XML}

/**
  *
  * @param endpoint The address of the OAI endpoint
  * @param metadataPrefix Metadata prefix to harvest
  * @param outDir Location to save the harvested records
  * @param scheme http or https, defaults to http
  */
class OaiHarvester (endpoint: String,
                   metadataPrefix: String,
                   outDir: File,
                   scheme: String = "http") {

  private[this] val logger = LogManager.getLogger("harvester")

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
    // TODO define Error classes
    val errorCode = (xml \\ "error" \ "@code").text
    if (errorCode.nonEmpty) { checkOaiErrorCode(errorCode) }

    val docs:NodeSeq = xml \\ "OAI-PMH" \\ "ListRecords" \\ "record"

    // Attempt at FP serialization
    docs.par
      .map(doc => {
        val provIdentifier = (doc \\ "header" \\ "identifier").text
        val dplaIdentifier = Harvester.generateMd5(provIdentifier)
        val filename = dplaIdentifier+".xml"
        val outFile = new File(outDir, filename)

        // TODO this might go someplace else...single responsibility principle...
        FileIO.writeFile(doc.text, outFile)
      })

    return (xml \\ "OAI-PMH" \\ verb \\ "resumptionToken").text
  }

  /**
    * Checks the error response codes and logs an appropriate message and
    * throws Exceptions
    * TODO define error classes and messages
    *
    * @param errorCode The error code from the OAI response
    *                  See https://www.openarchives.org/OAI/openarchivesprotocol.html#ErrorConditions
    */
  def checkOaiErrorCode(errorCode: String): Unit = {
    errorCode match {
      case "" => logger.info("No error")

      case "badArguement" => {
        logger.error("The request includes illegal arguments or is missing required arguments.")
        throw new Exception("Error harvest. Correct the metadataPrefix and restart.")
      }

      case "badResumptionToken" => {
        logger.error(s"The value of the resumptionToken argument is invalid or expired.")
        throw new Exception("BadResumptionToken in harvest request")
      }

      case "badVerb" => {
        logger.error(s"Value of the verb argument is not a legal OAI-PMH verb, the verb argument " +
          s"is missing, or the verb argument is repeated.")
        throw new Exception("BadVerb in harvest request")
      }

      case "cannotDisseminateFormat" => {
        logger.error("The value of the metadataPrefix argument is not supported by the repository.")
        throw new Exception("Error in harvest. Correct the metadataPrefix and restart.")
      }

      case "idDoesNotExist" => {
        logger.error("The value of the identifier argument is unknown or illegal in this repository.")
        throw new Exception("Error in harvest")
      }

      case "noRecordsMatch" => {
        // Does not throw an exception, this case will not break the harvest but result in a no-op
        logger.warn(s"No records returned from request")
      }

      case "noMetadataFormats" => {
        logger.error(s"Repository does not support sets. Please correct the ingestion profile and retry.")
      }

      case "noSetHierarchy" => {
        logger.error(s"Repository does not support sets. Please correct the ingestion profile and retry.")

        throw new Exception("Error in harvest")
      }
      // Base case, the error code doesn't match any of the expected values
      case _ => {
        logger.error(s"Unknown error code: ${errorCode}")
        throw new Exception(s"Unknown error code in harvest")
      }
    }
  }
}
