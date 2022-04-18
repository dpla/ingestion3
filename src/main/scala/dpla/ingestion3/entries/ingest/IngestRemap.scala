package dpla.ingestion3.entries.ingest

import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf, MachineLearningConf}
import dpla.ingestion3.dataStorage.InputHelper
import dpla.ingestion3.executors.{EnrichExecutor, JsonlExecutor, MappingExecutor, TopicModelExecutor, WikimediaMetadataExecutor}
import dpla.ingestion3.entries.reports.ReporterMain._
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf
/**
  * Single entry point to run harvest, mapping, enrichment, indexing jobs and all reports.
  * Support running against an already harvested data set and running a fresh harvest
  *
  * Expects four parameters:
  *   1)  --output  A base path to save the mapping, enrichment, json-l and report outputs
  *   2)  --input   A path to the harvested data
  *   3)  --conf    A path to the application configuration file
  *   4)  --name    Provider short name
  *   5)  --sparkMaster optional parameter that overrides a --master param submitted
  *                     via spark-submit (e.g. local[*])
  *   6) --stopWords    optional path to a txt file containing stopwords, default is set in application.conf
  *   7) --cvModel      optional path to a spark CountVectorizerModel, default is set in application.conf
  *   8) --ldaModel     optional path to a spark LDAModel (Latent Dirichlet Allocation),
  *                     default is set in application.conf
  *
  * To invoke via spark-submit:
  *
  * To get the stanford jar on the cluster, do:
  *   wget http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.9.1/stanford-corenlp-3.9.1-models.jar
  *
  * spark/bin/spark-submit
  *   --master [SPARK_MASTER]
  *   --driver-memory [DRIVER_MEMORY]
  *   --executor-memory [EXECUTOR_MEMORY]
  *   --class dpla.ingestion3.entries.ingest.IngestRemap
  *   --packages com.databricks:spark-avro_2.11:4.0.0,org.apache.hadoop:hadoop-aws:2.7.6, \
  *     com.amazonaws:aws-java-sdk:1.7.4,org.rogach:scallop_2.11:3.0.3,com.typesafe:config:1.3.1, \
  *     org.eclipse.rdf4j:rdf4j-model:2.2,org.jsoup:jsoup:1.10.2,org.eclipse.rdf4j:rdf4j-model:2.2, \
  *     databricks/spark-corenlp:0.3.1-s_2.11
  *   --jars stanford-corenlp-3.9.1-models.jar
  *   --conf spark.driver.extraClassPath=stanford-corenlp-3.9.1-models.jar
  *   --conf spark.executor.extraClassPath=stanford-corenlp-3.9.1-models.jar
  *   [PATH_TO_JAR]
  *   --input [PATH_TO_HARVEST]
  *   --output [OUTPUT_DIRECTORY]
  *   --conf [PATH_TO_CONF]
  *   --name [PROVIDER_SHORTNAME]
  *   --stopWords [OPTIONAL_PATH_TO_STOPWORDS]
  *   --cvModel [OPTIONAL_PATH_TO_CV_MODEL]
  *   --ldaModel [OPTIONAL_PATH_TO_LDA_MODEL]
  *
  */
object IngestRemap extends MappingExecutor
  with JsonlExecutor
  with EnrichExecutor
  with TopicModelExecutor
  with WikimediaMetadataExecutor {

  def main(args: Array[String]): Unit = {

    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val baseDataOut = cmdArgs.getOutput
    val confFile = cmdArgs.getConfigFile
    val shortName = cmdArgs.getProviderName
    val input = cmdArgs.getInput
    val sparkMaster: Option[String] = cmdArgs.getSparkMaster
    val stopWords: String = cmdArgs.getStopWords.getOrElse(MachineLearningConf.stopWordsPath)
    val cvModel: String = cmdArgs.getCvModel.getOrElse(MachineLearningConf.cvModelPath)
    val ldaModel: String = cmdArgs.getLdaModel.getOrElse(MachineLearningConf.ldaModelPath)

    // Get logger
    val logger = Utils.createLogger("ingest", shortName)

    // Outputs

    // If given input path is a harvest, use it as `harvestData'.
    // If not, assume that it is a directory containing several harvests and
    // get the most recent harvest from that directory.
    val harvestData = InputHelper.isActivityPath(input) match {
      case true => input
      case false => InputHelper.mostRecent(input)
        .getOrElse(throw new RuntimeException("Unable to load harvest data."))
    }

    logger.info(s"Using harvest data from $harvestData")

    // Load configuration from file.
    val i3Conf = new Ingestion3Conf(confFile, Some(shortName))
    val conf = i3Conf.load()

    val baseConf = new SparkConf()
      .setAppName(s"IngestRemap: $shortName")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max", "200")

    val sparkConf = sparkMaster match {
      case Some(m) => baseConf.setMaster(m)
      case None => baseConf
    }

    // TODO These processes should return some flag or metric to help determine whether to proceed
    // Mapping
    val mapDataOut: String =
      executeMapping(sparkConf, harvestData, baseDataOut, shortName, logger)

    // Enrichment
    val enrichDataOut: String =
      executeEnrichment(sparkConf, mapDataOut, baseDataOut, shortName, logger, conf)

    // Json-l
     executeJsonl(sparkConf, enrichDataOut, baseDataOut, shortName, logger)

    // Reports
    // Reports commented out by S. Williams for efficiency and because they aren't being used on a regular basis
    // executeAllReports(sparkConf, mapDataOut, baseDataOut, shortName, logger)

    // Topic models are no longer needed 
    // LDA vectors
    // executeTopicModel(sparkConf, enrichDataOut, baseDataOut, shortName, stopWords, cvModel, ldaModel, logger)

    // Wikimedia
    executeWikimediaMetadata(sparkConf, enrichDataOut, baseDataOut, shortName, logger)
  }
}
