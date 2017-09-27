package dpla.ingestion3

import java.io.{File, PrintWriter}

import dpla.ingestion3.mappers.providers._
import dpla.ingestion3.model.RowConverter
import dpla.ingestion3.utils.Utils
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import com.databricks.spark.avro._
import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, i3Conf}

import scala.util.{Failure, Success}


/**
  * Expects four parameters:
  * 1) a path to the harvested data
  * 2) a path to output the mapped data
  * 3) a path to the configuration file
  * 4) provider short name (e.g. 'mdl', 'cdl', 'harvard')
  *
  * Usage
  * -----
  * To invoke via sbt:
  * sbt "run-main dpla.ingestion3.MappingEntry
  *       --input=/input/path/to/harvested/
  *       --output=/output/path/to/mapped/
  *       --conf=/path/to/conf
  *       --name=shortName"
  */

object MappingEntry {

  def main(args: Array[String]): Unit = {
    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val dataIn = cmdArgs.input.toOption
      .map(_.toString)
      .getOrElse(throw new RuntimeException("No input data specified."))
    val dataOut = cmdArgs.output.toOption
      .map(_.toString)
      .getOrElse(throw new RuntimeException("No output location specified."))
    val confFile = cmdArgs.configFile.toOption
      .map(_.toString)
      .getOrElse(throw new RuntimeException("No conf file specified."))
    val shortName = cmdArgs.providerName.toOption
      .map(_.toString)
      .getOrElse(throw new RuntimeException("No provider short name specified."))

    // Get logger.
    val mappingLogger: Logger = LogManager.getLogger("ingestion3")
    val appender = Utils.getFileAppender(shortName, "mapping")
    mappingLogger.addAppender(appender)

    // Log config file location and provider short name.
    mappingLogger.info(s"Mapping initiated")
    mappingLogger.info(s"Config file: ${confFile}")
    mappingLogger.info(s"Provider short name: ${shortName}")

    // Load configuration from file.
    val i3Conf = new Ingestion3Conf(confFile, Some(shortName))
    val conf: i3Conf = i3Conf.load()

    // Read spark master property from conf, default to 'local[1]' if not set
    val sparkMaster = conf.spark.sparkMaster.getOrElse("local[1]")

    val sparkConf = new SparkConf()
      .setAppName(s"Mapping: ${shortName}")
      .setMaster(sparkMaster)

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // Need to keep this here despite what IntelliJ and Codacy say
    import spark.implicits._

    //these three Encoders allow us to tell Spark/Catalyst how to encode our data in a DataSet.
    val oreAggregationEncoder: ExpressionEncoder[Row] = RowEncoder(model.sparkSchema)
    val stringEncoder: ExpressionEncoder[String] = ExpressionEncoder()
    val tupleRowStringEncoder: ExpressionEncoder[Tuple2[Row, String]] =
      ExpressionEncoder.tuple(RowEncoder(model.sparkSchema), ExpressionEncoder())

    // Load the harvested record dataframe
    val harvestedRecords: DataFrame = spark.read.avro(dataIn)
    
    // Match on the shortName to select the correct Extractor
    val extractorClass = shortName match {
      case "cdl" => classOf[CdlExtractor]
      case "mdl" => classOf[MdlExtractor]
      case "nara" => classOf[NaraExtractor]
      case "pa" => classOf[PaExtractor]
      case "wi" => classOf[WiExtractor]
      case _ =>
        mappingLogger.fatal(s"No match found for provider short name ${shortName}")
        throw new Exception("Cannot find a mapper")
    }

    // Run the mapping over the Dataframe
    val documents: Dataset[String] = harvestedRecords.select("document").as[String]
    val mappingResults: Dataset[(Row, String)] = documents.map(document =>
      map(extractorClass, document, shortName))(tupleRowStringEncoder)

    // Delete the output location if it exists
    Utils.deleteRecursively(new File(dataOut))


    val successResults: Dataset[Row] = mappingResults
          .filter(tuple => Option(tuple._1).isDefined)
          .map(tuple => tuple._1)(oreAggregationEncoder)

    val failures:  Array[String] = mappingResults
      .filter(tuple => Option(tuple._2).isDefined)
      .map(tuple => tuple._2).collect()

    successResults.toDF().write.avro(dataOut)


    // Summarize results
    mappingSummary(harvestedRecords.count(), successResults.count(), failures, dataOut, shortName)

    spark.stop()
  }

  private def map(extractorClass: Class[_ <: Extractor], document: String, shortName: String): (Row, String) =
    extractorClass.getConstructor(classOf[String]).newInstance(document, shortName).build() match {
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
