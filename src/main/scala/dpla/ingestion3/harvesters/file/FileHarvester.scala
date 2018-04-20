package dpla.ingestion3.harvesters.file

import java.io.File

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.api.{ApiError, ApiRecord, ApiResponse}
import dpla.ingestion3.utils.{AvroUtils, FlatFileIO}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
  * File base harvester
  *
  * @param shortName
  * @param conf
  * @param outputDir
  * @param logger
  */
abstract class FileHarvester(shortName: String,
                             conf: i3Conf,
                             outputDir: String,
                             logger: Logger)
  extends Harvester(shortName, conf, outputDir, logger) {


  val schemaStr: String = new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc")
  val schema: Schema = new Schema.Parser().parse(schemaStr)

  protected def localFileHarvest: Unit

  /**
    * This is lazy b/c queryParams should be printed before avroWriter is set.
    * @see doHarvest
    */
  protected lazy val avroWriter: DataFileWriter[GenericRecord] = {
    val dirName = if (outputDir.endsWith("/")) outputDir.dropRight(1) else outputDir
    new File(dirName).mkdirs
    val fileName = dirName + "/file_harvest.avro"
    val file = new File(fileName)
    AvroUtils.getAvroWriter(file, schema)
  }

  /**
    * Generalized driver for ApiHarvesters invokes localApiHarvest() method and reports
    * summary information.
    */
  protected def runHarvest: Try[DataFrame] = Try {

    avroWriter.setFlushOnEveryBlock(true)

    // Calls the local implementation
    localFileHarvest

    avroWriter.close()
    logger.info(s"Records saved to $outputDir")

    spark.read.avro(outputDir)
  }

  /**
    * Saves the records
    *
    * @param docs - List of ApiRecords to save out
    */
  protected def saveOut(docs: List[ApiRecord]): Unit = {

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
  }

  def saveOutErrors(errors: List[ApiError]): Unit = {
    errors.foreach(error => {
      logger.error(s"URL: ${error.errorSource.url.get}\nMessage: ${error.message} \n\n")
    })
  }

  protected def saveOutAll(msgs: List[ApiResponse]): Unit = {

    val docs = msgs.collect { case a: ApiRecord => a }
    val errors = msgs.collect { case a: ApiError => a }

    saveOut(docs)
    saveOutErrors(errors)
  }

}
