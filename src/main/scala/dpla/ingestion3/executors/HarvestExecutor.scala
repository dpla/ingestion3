package dpla.ingestion3.executors

import java.time.LocalDateTime
import dpla.ingestion3.confs.i3Conf
import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.harvesters.Harvester
import dpla.ingestion3.harvesters.file.NaraDeltaHarvester
import dpla.ingestion3.harvesters.oai.OaiHarvester
import dpla.ingestion3.model.harvestAvroSchema
import dpla.ingestion3.utils.{CHProviderRegistry, Utils}
import org.apache.log4j.Logger
import org.apache.logging.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}

trait HarvestExecutor {

  /** Run the appropriate type of harvest.
    *
    * @param shortName
    *   Provider short name (e.g. cdl, mdl, nara, loc).
    * @see
    *   CHProviderRegistry.register() for the authoritative list of provider
    *   short names.
    * @param conf
    *   Configurations read from application configuration file
    */
  def execute(
      sparkConf: SparkConf,
      shortName: String,
      dataOut: String,
      conf: i3Conf
  ): Unit = {

    val logger = LogManager.getLogger(this.getClass)

    // Log config file location and provider short name.
    logger.info(s"Harvest initiated")
    logger.info(s"Provider short name: $shortName")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    // Get and log harvest type.
    val harvestType = conf.harvest.harvestType
      .getOrElse(throw new RuntimeException("No harvest type specified."))
    logger.info(s"Harvest type: $harvestType")

    val harvester = buildHarvester(spark, shortName, conf, harvestType)

    // This start time is used for documentation and output file naming.
    val startDateTime = LocalDateTime.now

    // This start time is used to measure the duration of harvest.
    val start = System.currentTimeMillis()

    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "harvest", startDateTime)

    val outputPath = outputHelper.activityPath

    // Call local implementation of runHarvest()
    Try {
      // Calls the local implementation
      val harvestData: DataFrame = harvester.harvest

      // if there are setIds in the returned dataframe then generate a count summary by setId
      val setSummary: Option[String] =
        if (harvestData.columns.contains("setIds")) {
          val summary = harvestData
            .groupBy("setIds")
            .count()
            .sort("setIds")
            .collect()
            .map(row => row.getSeq[String](0).mkString(" ") -> row.getLong(1))
            .map { case (set: String, count: Long) => s"$set, $count" }
            .mkString("\n")

          // drop setIds column from dataframe
          harvestData.drop("setIds")

          Some(summary)
        } else {
          None
        }

      // Write harvested data to output file.
      harvestData.write
        .format("avro")
        .option("avroSchema", harvestAvroSchema.toString)
        .save(outputPath)

      setSummary match {
        case Some(s) =>
          outputHelper.writeSetSummary(s) match {
            case Success(s) => logger.info(s"OAI set summary written to $s.")
            case Failure(f) => print(f.toString)
          }
        case None =>
      }

      // Reads the saved avro file back
      spark.read.format("avro").load(outputPath)
    } match {
      case Success(df) =>
        Harvester.validateSchema(df)
        val recordCount = df.count()

        logger.info(
          Utils.harvestSummary(
            outputPath,
            System.currentTimeMillis() - start,
            recordCount
          )
        )

        val manifestOpts: Map[String, String] = Map(
          "Activity" -> "Harvest",
          "Provider" -> shortName,
          "Record count" -> recordCount.toString
        )

        outputHelper.writeManifest(manifestOpts) match {
          case Success(s) => logger.info(s"Manifest written to $s.")
          case Failure(f) => logger.warn(s"Manifest failed to write.", f)
        }

      case Failure(f) => logger.error(s"Harvest failure.", f)
    }
    harvester.cleanUp()
    spark.stop()
  }

  private def buildHarvester(
      spark: SparkSession,
      shortName: String,
      conf: i3Conf,
      harvestType: String
  ) = {
    harvestType match {
      case "oai" =>
        new OaiHarvester(spark, shortName, conf)
      case "nara.file.delta" =>
        new NaraDeltaHarvester(spark, shortName, conf)
      case "api" | "file" =>
        val harvesterClass =
          CHProviderRegistry.lookupHarvesterClass(shortName) match {
            case Success(harvClass) => harvClass
            case Failure(e) =>
              throw e
          }
        harvesterClass
          .getConstructor(
            classOf[SparkSession],
            classOf[String],
            classOf[i3Conf],
            classOf[Logger]
          )
          .newInstance(spark, shortName, conf)
      case _ =>
        throw new RuntimeException("Can't find harvest type " + harvestType)
    }
  }
}
