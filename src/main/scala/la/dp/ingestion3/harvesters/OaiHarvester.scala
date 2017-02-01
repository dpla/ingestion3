package la.dp.ingestion3.harvesters

import java.io.File
import java.net.URL

import la.dp.ingestion3.OaiQueryUrlBuilder
import la.dp.ingestion3.utils.FileIO
import org.apache.http.client.utils.URIBuilder
import org.apache.log4j.LogManager

import scala.xml.{NodeSeq, XML}

/**
  * OAI-PMH harvester for aggregating metadata into the DPLA
  *
  * @param outDir File
  *               Location to save files
  *               TODO: this should be 100% agnostic S3, local, network
  * @param fileIO FileIO
  *               The protocol to use to save the records (FlatFileIO, SeqFileIO)
  */
class OaiHarvester (outDir: File,
                    fileIO: FileIO) {

  // Logging object
  private[this] val logger = LogManager.getLogger("OAI harvester")

  /**
    * Testing the apply method
    *
    * @param url
    * @return
    */
  def apply(url: URL) = runHarvest(url)

  /**
    * Takes the XML response from a List[Sets,Records] request and processes it
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
    * Get the error property if it exists
    *
    * @return Option[String]
    *         The error code if it exists otherwise empty string
    */
  def getOaiErrorCode(xml: NodeSeq): Option[String] = {
    // TODO this is redundant but I don't know how to annonymously
    // TODO the xml select
    val errorCode = (xml \\ "OAI-PMH" \\ "error").text

    errorCode match {
      case "" => None
      case _ => Some(errorCode)
    }
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
    * Executes the request and returns the response
    *
    * @param url URL
    *            OAI request URL
    * @return NodeSeq
    *         XML response
    */
  @throws(classOf[HarvesterException])
  def getXmlResponse(url: URL): NodeSeq = {
    XML.load(url)
  }

  /**
    * Makes a single request and saves records in resposne to disk
    *
    * @param url URL
    *            Complete request URL to make of the OAI endpoint
    *
    * @return String
    *         resumptionToken
    */
  @throws(classOf[Exception])
  def runHarvest(url: URL): String = {
    val xml = getXmlResponse(url)

    // Get and check the error code if it exists
    getOaiErrorCode(xml) match {
      case Some(e) => throw HarvesterException(s"Request failed: ${url.toString} \n"+e)
      case None => logger.info(s"Request successful: ${url.toString}")
    }
    // Transform the XML response into a Map[File,String] and write to disk
    val docMap: Map[File, String] = getHarvestedRecords(xml)
    // TODO ...move this out...I could return an OaiResult object that would
    // TODO hold resTok, output map or error.. need to think about that
    fileIO.writeFiles(docMap)
    // Return the resumptionToken
    getResumptionToken(xml)
  }
}
