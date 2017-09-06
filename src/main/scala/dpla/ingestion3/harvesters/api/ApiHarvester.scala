package dpla.ingestion3.harvesters.api

import java.io.File

import dpla.ingestion3.utils.Utils
import org.apache.avro.Schema
import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro._
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}


trait ApiHarvester {

  /**
    * Method for the provider specific harvester to implement. This is the bulk
    * of the harvester and should contain the query, ID selection, ApiRecord construction
    * and loop control.
    *
    * @param queryParams Map of query parameters
    * @param avroWriter Writer
    * @param schema Schema to apply to data
    */
  def doHarvest(queryParams: Map[String, String],
                avroWriter: DataFileWriter[GenericRecord],
                schema: Schema)

  /**
    * Saves the records
    *
    * @param avroWriter -
    * @param docs - List of ApiRecords to save out
    * @param schema - Avro Schema to apply
    * @param shortName - Provider's abbreviated name [e.g. cdl, mdl, nara]
    * @param mimeType - application_json, application_xml
    */
  def saveOut(avroWriter: DataFileWriter[GenericRecord],
              docs: List[ApiRecord],
              schema: Schema,
              shortName: String,
              mimeType: String): Unit = {
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

  /**
    * Generalized driver for ApiHarvesters invokes the local doHarvest() method and reports
    * summary information
    *
    * @param queryParams
    * @param avroWriter
    * @param schema
    */
  def startHarvest(outFile: File,
                   queryParams: Map[String,String],
                   avroWriter: DataFileWriter[GenericRecord],
                   schema: Schema): Unit = {

    avroWriter.setFlushOnEveryBlock(true)

    // Runtime tracking
    val startTime = System.currentTimeMillis()

    // Calls the local implementation
    doHarvest(queryParams, avroWriter, schema)

    avroWriter.close()

    val endTime = System.currentTimeMillis()

    val spark = SparkSession.builder().master("local").getOrCreate()
    val recordCount = spark.read.avro(outFile.toString).count()
    spark.stop()

    // Summarize results
    Utils.printResults(endTime-startTime, recordCount)
  }

}
