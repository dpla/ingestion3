package dpla.ingestion3.harvesters.api

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{Harvester, UrlBuilder}
import org.apache.avro.generic.GenericData
import org.apache.log4j.Logger

/**
  * API base harvester
  *
  * @param shortName Provider short name
  * @param conf      Configurations
  * @param outputDir Output path
  * @param logger    Logger
  */
abstract class ApiHarvester(shortName: String,
                            conf: i3Conf,
                            outputDir: String,
                            logger: Logger)
  extends Harvester(shortName, conf, outputDir, logger)
    with UrlBuilder {

  // Abstract method queryParams should set base query parameters for API call.
  protected val queryParams: Map[String, String]

  override protected val filename: String = s"${shortName}_${System.currentTimeMillis()}" +
    ".avro"

  /**
    * Writes errors and documents to log file and avro file respectively
    *
    * @param msgs List[ApiResponse]
    */
  protected def saveOutAll(msgs: List[ApiResponse]): Unit = {
    val docs = msgs.collect { case a: ApiRecord => a }
    val errors = msgs.collect { case a: ApiError => a }

    saveOutRecords(docs)
    saveOutErrors(errors)
  }

  /**
    * Saves the records
    *
    *
    * @param docs - List of ApiRecords to save out
    */
  protected def saveOutRecords(docs: List[ApiRecord]): Unit =
    // TODO Integrate this with File harvester save out methods 
    docs.foreach(doc => {
      val startTime = System.currentTimeMillis()
      val unixEpoch = startTime / 1000L

      val genericRecord = new GenericData.Record(schema)

      genericRecord.put("id", doc.id)
      genericRecord.put("ingestDate", unixEpoch)
      genericRecord.put("provider", shortName)
      genericRecord.put("document", doc.document)
      genericRecord.put("mimetype", mimeType)
      avroWriter.append(genericRecord)
    })

  /**
    * Writes errors out to log file
    *
    * @param errors List[ApiErrors}
    */
  protected def saveOutErrors(errors: List[ApiError]): Unit =
    errors.foreach(error => {
      logger.error(s"URL: ${error.errorSource.url.getOrElse("No url")}" +
        s"\nMessage: ${error.message} \n\n")
    })

}
