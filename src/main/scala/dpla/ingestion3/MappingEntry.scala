package dpla.ingestion3

import java.io.File

import dpla.ingestion3.mappers.providers.{CdlExtractor, NaraExtractor}
import dpla.ingestion3.model.DplaMapData
import dpla.ingestion3.utils.Utils
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Expects two parameters:
  *   1) a path to the harvested data
  *   2) a path to output the mapped data
  *
  *   Usage
  *   -----
  *   To invoke via sbt:
  *     sbt "run-main dpla.ingestion3.MappingEntry /input/path/to/harvested.avro /output/path/to/mapped.avro"
  */

object MappingEntry {

  def main(args: Array[String]): Unit = {
    val logger = LogManager.getLogger(MappingEntry.getClass)

    if (args.length != 2)
      logger.error("Incorrect number of parameters provided. Expected <input> <output>")

    // Get files
    val dataIn = args(0)
    val dataOut = args(1)

    val sparkConf = new SparkConf()
      .setAppName("Mapper")
      // TODO there should be a central place to store the sparkMaster
      .setMaster("local[*]")
      // TODO: This spark.serializer is a kludge to get around serialization issues. Will be fixed in future ticket
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    implicit val dplaMapDataEncoder = org.apache.spark.sql.Encoders.kryo[DplaMapData]

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._

    // Load the harvested record dataframe
    val harvestedRecords = spark.read
      .format("com.databricks.spark.avro")
      .load(dataIn)

    // Take the first harvested record in the dataFrame and get the provider value
    val shortName = harvestedRecords.take(1)(0).getAs[String]("provider").toLowerCase

    // Match on the shortName to select the correct Extractor
    val extractorClass = shortName match {
      case "cdl" => classOf[CdlExtractor]
      case "nara" => classOf[NaraExtractor]
    }

    // Run the mapping over the Dataframe
    val mappedRecords = harvestedRecords.map(
      record => {
        extractorClass.getConstructor(classOf[String])
          .newInstance(record.getAs[String]("document"))
          .build
      }
    )

    // Delete the output location if it exists
    Utils.deleteRecursively(new File(dataOut))

    // Save mapped records out to Avro file
    mappedRecords.toDF("document").write
      .format("com.databricks.spark.avro")
      .save(dataOut)

    // Gather some stats
    val harvestedRecordCount = harvestedRecords.count()
    val mappedRecordCount = mappedRecords.count()

    logger.debug(s"Harvested ${harvestedRecordCount} records and mapped ${mappedRecordCount} records")
    logger.debug(s"${harvestedRecordCount-mappedRecordCount} mapping errors")
  }
}
