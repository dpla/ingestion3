package dpla.ingestion3.executors

import java.time.LocalDateTime

import com.databricks.spark.avro._
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.oai.OaiHarvester
import dpla.ingestion3.harvesters.pss.PssHarvester
import dpla.ingestion3.harvesters.resourceSync.RsHarvester
import dpla.ingestion3.utils.{OutputHelper, ProviderRegistry, Utils}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

trait HarvestExecutor {

  /**
    * Run the appropriate type of harvest.
    *
    * @param shortName Provider short name (e.g. cdl, mdl, nara, loc).
    * @see ProviderRegistry.register() for the authoritative
    *      list of provider short names.
    * @param outputDir Location to save output from harvest
    * @param conf      Configurations read from application configuration file
    * @param logger    Logger object
    * @return Try[Long] The number of successfully harvested records
    */
  def execute(sparkConf: SparkConf,
              shortName: String,
              dataOut: String,
              conf: i3Conf,
              logger: Logger): Try[Long] = {

    // Log config file location and provider short name.
    logger.info(s"Harvest initiated")
    logger.info(s"Provider short name: $shortName")

    //todo build spark here
    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    // Get and log harvest type.
    val harvestType = conf.harvest.harvestType
      .getOrElse(throw new RuntimeException("No harvest type specified."))
    logger.info(s"Harvest type: $harvestType")

    val harvester = buildHarvester(spark, shortName, conf, logger, harvestType)

    // This start time is used for documentation and output file naming.
    val startDateTime = LocalDateTime.now

    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "harvest", startDateTime)

    // This start time is used to measure the duration of harvest.
    val start = System.currentTimeMillis()

    // Call local implementation of runHarvest()
    val result: Try[Long] = Try {
      // Calls the local implementation
      val harvestData: DataFrame = harvester.harvest
      val outputPath = outputHelper.outputPath

      // Write harvested data to output file.
      harvestData
        .write
        .format("com.databricks.spark.avro")
        .option("avroSchema", harvestData.schema.toString)
        .avro(outputPath)

      logger.info(s"Saving to $outputPath")

      // Reads the saved avro file back
      spark.read.avro(outputPath)
    } match {
      case Success(df) =>
        Harvester.validateSchema(df)
        val recordCount = df.count()
        logger.info(Utils.harvestSummary(System.currentTimeMillis() - start, recordCount))

        val manifestOpts: Map[String, String] = Map(
          "Activity" -> "Harvest",
          "Provider" -> shortName,
          "Record count" -> recordCount.toString
        )
        outputHelper.writeManifest(manifestOpts)

        Success(recordCount)
      case Failure(f) => Failure(f)
    }

    spark.stop()
    result
  }

  private def buildHarvester(spark: SparkSession, shortName: String, conf: i3Conf, logger: Logger, harvestType: String) = {
    harvestType match {
      case "oai" =>
        new OaiHarvester(spark, shortName, conf, logger)
      case "pss" =>
        new PssHarvester(spark, shortName, conf, logger)
      case "rs" =>
        new RsHarvester(spark, shortName, conf, logger)
      case "api" | "file" =>
        val harvesterClass = ProviderRegistry.lookupHarvesterClass(shortName) match {
          case Success(harvClass) => harvClass
          case Failure(e) =>
            logger.fatal(e.getMessage)
            throw e
        }
        harvesterClass
          .getConstructor(classOf[SparkSession], classOf[String], classOf[i3Conf], classOf[Logger])
          .newInstance(spark, shortName, conf, logger)

      case _ =>
        val msg = s"Harvest type not recognized."
        logger.fatal(msg)
        throw new RuntimeException(msg)
    }
  }

}
