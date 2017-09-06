package dpla.ingestion3

import java.io.File

import dpla.ingestion3.mappers.providers.{CdlExtractor, NaraExtractor, PaExtractor}
import dpla.ingestion3.model.{DplaMap, DplaMapData, DplaMapError}
import dpla.ingestion3.utils.Utils
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import java.io._

/**
  * Expects two parameters:
  *   1) a path to the harvested data
  *   2) a path to output the mapped data
  *
  *   Usage
  *   -----
  *   To invoke via sbt:
  *     sbt "run-main dpla.ingestion3.MappingEntry /input/path/to/harvested/ /output/path/to/mapped/"
  */

object MappingEntry {

  def main(args: Array[String]): Unit = {
    val logger = LogManager.getLogger(MappingEntry.getClass)

    if (args.length != 2)
      logger.error("Incorrect number of parameters provided. Expected <input> <output>")

    // Get the input and output paths
    val dataIn = args(0)
    val dataOut = args(1)

    val sparkConf = new SparkConf()
      .setAppName("Mapper")
      // TODO there should be a central place to store the sparkMaster
      .setMaster("local[*]")
      // TODO: This spark.serializer is a kludge to get around serialization issues. Will be fixed in future ticket
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    // Needed to serialize the successful mapped records
    implicit val dplaMapDataEncoder = org.apache.spark.sql.Encoders.kryo[DplaMapData]
    // Need to map the mapping results
    implicit val dplaMapEncoder = org.apache.spark.sql.Encoders.kryo[DplaMap]

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
      case "pa digital" => classOf[PaExtractor]
      case _ =>
        logger.fatal(s"No match found for provider short name ${shortName}")
        throw new Exception("Cannot find a mapper")
    }

    // Run the mapping over the Dataframe
    val mappingResults = harvestedRecords.select("document").map(
      record => {
        extractorClass.getConstructor(classOf[String])
          .newInstance(record.getAs[String]("document"))
          .build
      }
    )

    // TODO there is probably a much cleaner/better way of writing this.
    val mappingSuccess = mappingResults
      .filter(r => r.isInstanceOf[DplaMapData])
      .map(r2 => r2.asInstanceOf[DplaMapData])

    // TODO ditto ^^ This converts the Dataset to an Array[String], This is an easy way out for now.
    val failures = mappingResults
      .filter(r => r.isInstanceOf[DplaMapError])
      .map(r2 => r2.asInstanceOf[DplaMapError]).map(f => f.errorMessage).collect()

    // Delete the output location if it exists
    Utils.deleteRecursively(new File(dataOut))

    // Save successfully mapped records out to Avro file
    mappingSuccess.toDF("document").write
      .format("com.databricks.spark.avro")
      .save(dataOut)

    // Summarize results
    mappingSummary(harvestedRecords.count(), mappingSuccess.count(),failures, dataOut, shortName)
  }

  /**
    * Print mapping summary information
    *
    * @param harvestCount
    * @param mapCount
    * @param errors
    * @param outDir
    * @param shortName
    */
  def mappingSummary(harvestCount: Long,
                     mapCount: Long,
                     errors: Array[String],
                     outDir: String,
                     shortName: String): Unit = {
    val logDir = new File(s"$outDir/logs/")
    logDir.mkdirs()

    println(s"Harvested $harvestCount records")
    println(s"Mapped ${mapCount} records")
    println(s"Failed to map ${harvestCount-mapCount} records.")
    if (mapCount != harvestCount)
      println(s"Saving error log to ${logDir.getAbsolutePath}")
      val pw = new PrintWriter(
        new File(s"${logDir.getAbsolutePath}/$shortName-mapping-errors-${System.currentTimeMillis()}.log"))
      errors.foreach(f => pw.write(s"$f\n"))
      pw.close()
  }
}
