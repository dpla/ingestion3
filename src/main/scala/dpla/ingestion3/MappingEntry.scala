package dpla.ingestion3

import java.io.{File, PrintWriter}

import dpla.ingestion3.mappers.providers._
import dpla.ingestion3.model.{DplaMapData, RowConverter}
import dpla.ingestion3.utils.Utils
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import com.databricks.spark.avro._

import scala.util.{Failure, Success}


/**
  * Expects two parameters:
  * 1) a path to the harvested data
  * 2) a path to output the mapped data
  *
  * Usage
  * -----
  * To invoke via sbt:
  * sbt "run-main dpla.ingestion3.MappingEntry /input/path/to/harvested/ /output/path/to/mapped/"
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

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._

    //these three Encoders allow us to tell Spark/Catalyst how to encode our data in a DataSet.
    val dplaMapDataRowEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)
    val stringEncoder: ExpressionEncoder[String] = ExpressionEncoder()
    val tupleRowStringEncoder: ExpressionEncoder[Tuple2[Row, String]] =
      ExpressionEncoder.tuple(RowEncoder(model.sparkSchema), ExpressionEncoder())

    // Load the harvested record dataframe
    val harvestedRecords: DataFrame = spark.read.avro(dataIn)

    // Take the first harvested record in the dataFrame and get the provider value
    val shortName = harvestedRecords.take(1)(0).getAs[String]("provider").toLowerCase

    // Match on the shortName to select the correct Extractor
    val extractorClass = shortName match {
      case "cdl" => classOf[CdlExtractor]
      case "mdl" => classOf[MdlExtractor]
      case "nara" => classOf[NaraExtractor]
      case "pa digital" => classOf[PaExtractor]
      case "wi" => classOf[WiExtractor]
      case _ =>
        logger.fatal(s"No match found for provider short name ${shortName}")
        throw new Exception("Cannot find a mapper")
    }

    // Run the mapping over the Dataframe
    val documents: Dataset[String] = harvestedRecords.select("document").as[String]
    val mappingResults: Dataset[(Row, String)] = documents.map(document => map(extractorClass, document))(tupleRowStringEncoder)

    // Delete the output location if it exists
    Utils.deleteRecursively(new File(dataOut))


    val successResults: Dataset[Row] = mappingResults
          .filter(tuple => Option(tuple._1).isDefined)
          .map(tuple => tuple._1)(dplaMapDataRowEncoder)

    val failures:  Array[String] = mappingResults
      .filter(tuple => Option(tuple._2).isDefined)
      .map(tuple => tuple._2).collect()

    successResults.toDF().write.avro(dataOut)


    // Summarize results
    mappingSummary(harvestedRecords.count(), successResults.count(), failures, dataOut, shortName)

    spark.stop()
  }

  private def map(extractorClass: Class[_ <: Extractor], document: String): (Row, String) =
    extractorClass.getConstructor(classOf[String]).newInstance(document).build() match {
      case Success(dplaMapData) => (RowConverter.toRow(dplaMapData, model.sparkSchema), null)
      case Failure(exception) => (null, exception.getMessage)
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
    println(s"Failed to map ${harvestCount - mapCount} records.")
    if (mapCount != harvestCount)
      println(s"Saving error log to ${logDir.getAbsolutePath}")
    val pw = new PrintWriter(
      new File(s"${logDir.getAbsolutePath}/$shortName-mapping-errors-${System.currentTimeMillis()}.log"))
    errors.foreach(f => pw.write(s"$f\n"))
    pw.close()
  }
}
