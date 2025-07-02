package dpla.ingestion3.harvesters.api

import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.{Harvester, LocalHarvester, ParsedResult}
import org.apache.avro.generic.GenericData
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession

/** API base harvester
  *
  * @param shortName
  *   Provider short name
  * @param conf
  *   Configurations
  */
abstract class ApiHarvester(
    shortName: String,
    conf: i3Conf
) extends LocalHarvester(shortName, conf) {

  // Abstract method queryParams should set base query parameters for API call.
  protected val queryParams: Map[String, String]

  override def cleanUp(): Unit = ()

  /** Writes errors and documents to log file and avro file respectively
    *
    * @param msgs
    *   List[ApiResponse]
    */
  protected def saveOutAll(msgs: List[ApiResponse]): Unit = {
    val docs = msgs.collect { case a: ApiRecord => a }
    val errors = msgs.collect { case a: ApiError => a }

    saveOutRecords(docs)
    saveOutErrors(errors)
  }

  /** Saves the records
    *
    * @param docs
    *   \- List of ApiRecords to save out
    */
  protected def saveOutRecords(docs: List[ApiRecord]): Unit = {
    val startTime = System.currentTimeMillis()
    val unixEpoch = startTime / 1000L

    docs.foreach(doc => {
      writeOut(unixEpoch, ParsedResult(
        doc.id,
        doc.document,
      ))
    })
  }

  private val logger = LogManager.getLogger(this.getClass)


  /** Writes errors out to log file
    *
    * @param errors
    *   List[ApiErrors]
    */
  private def saveOutErrors(errors: List[ApiError]): Unit =
    errors.foreach(error => {
        logger.error(
          s"URL: ${error.errorSource.url.getOrElse("No url")}" +
            s"\nMessage: ${error.message} \n\n"
        )
    })

}
