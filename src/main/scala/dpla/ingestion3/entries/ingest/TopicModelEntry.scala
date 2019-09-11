package dpla.ingestion3.entries.ingest

import dpla.ingestion3.confs.{CmdArgs, MachineLearningConf}
import dpla.ingestion3.executors.TopicModelExecutor
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf

/**
  * Driver for reading DplaMapData records (enriched) and generating topic model distributions.
  *
  * Expects seven parameters:
  * 1) a path to the enriched data
  * 2) a path to output the topic model data
  * 3) provider short name (e.g. 'mdl', 'cdl', 'harvard')
  * 4) a path to a txt file containing stopwords - optional, default is set in application.conf
  * 5) a path to a spark CountVectorizerModel - optional, default is set in application.conf
  * 6) a path to a spark LDAModel (Latent Dirichlet Allocation) - optional, default is set in application.conf
  * 7) spark master optional parameter that overrides a --master param submitted
  *    via spark-submit
  *
  * Usage
  * -----
  * To invoke via spark-submit:
  *
  * To get the stanford jar on the cluster, do:
  *   wget http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.9.1/stanford-corenlp-3.9.1-models.jar
  *
  * spark/bin/spark-submit
  *   --master [SPARK_MASTER]
  *   --driver-memory [DRIVER_MEMORY]
  *   --executor-memory [EXECUTOR_MEMORY]
  *   --class dpla.ingestion3.entries.ingest.TopicModelEntry
  *   --packages org.apache.hadoop:hadoop-aws:2.7.6,com.amazonaws:aws-java-sdk:1.7.4, \
  *     databricks/spark-corenlp:0.3.1-s_2.11,org.rogach:scallop_2.11:3.0.3, \
  *     com.databricks:spark-avro_2.11:4.0.0
  *   --jars stanford-corenlp-3.9.1-models.jar
  *   --conf spark.driver.extraClassPath=stanford-corenlp-3.9.1-models.jar
  *   --conf spark.executor.extraClassPath=stanford-corenlp-3.9.1-models.jar
  *   [PATH_TO_JAR]
  *   --input [INPUT]
  *   --output [OUTPUT_DIRECTORY]
  *   --name [PROVIDER_SHORTNAME]
  *   --stopWords [PATH_TO_STOPWORDS]
  *   --cvModel [PATH_TO_CV_MODEL]
  *   --ldaModel [PATH_TO_LDA_MODEL]
  *
  * To invoke via sbt:
  * sbt "run-main dpla.ingestion3.entries.ingest.TopicModelEntry
  *       --input=/input/path/to/enriched/
  *       --output=/output/path/to/topic/model/
  *       --name=shortName"
  *       --stopWords=/path/to/stop/words/
  *       --cvModel=/path/to/count/vectorizer/model/
  *       --ldaModel=/path/to/LDA/model/
  *       --sparkMaster=local[*]
  */
object TopicModelEntry extends TopicModelExecutor {

  def main(args: Array[String]): Unit = {

    // Read in command line args.
    val cmdArgs = new CmdArgs(args)

    val dataIn: String = cmdArgs.getInput
    val dataOut: String = cmdArgs.getOutput
    val shortName: String = cmdArgs.getProviderName
    val stopWords: String = cmdArgs.getStopWords.getOrElse(MachineLearningConf.stopWordsPath)
    val cvModel: String = cmdArgs.getCvModel.getOrElse(MachineLearningConf.cvModelPath)
    val ldaModel: String = cmdArgs.getLdaModel.getOrElse(MachineLearningConf.ldaModelPath)
    val sparkMaster: Option[String] = cmdArgs.getSparkMaster

    val baseConf =
      new SparkConf()
        .setAppName(s"Topic Model: $shortName")

    val sparkConf = sparkMaster match {
      case Some(m) => baseConf.setMaster(m)
      case None => baseConf
    }

    val logger = Utils.createLogger("machine_learning")

    executeTopicModel(sparkConf, dataIn, dataOut, shortName, stopWords, cvModel, ldaModel, logger)
  }
}
