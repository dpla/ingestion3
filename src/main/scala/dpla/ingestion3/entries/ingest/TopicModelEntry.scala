package dpla.ingestion3.entries.ingest

import dpla.ingestion3.confs.CmdArgs
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
  * 4) a path to a txt file containing stopwords
  * 5) a path to a spark CountVectorizerModel
  * 6) a path to a spark LDAModel (Latent Dirichlet Allocation)
  * 7) spark master (optional parameter that overrides a --master param submitted
  *    via spark-submit
  *
  * Usage
  * -----
  * To invoke via spark-submit:
  *
  * /**
  * * Sample invocation
  * *
  * * To get the stanford jar on the cluster, do:
  * *   wget http://repo1.maven.org/maven2/edu/stanford/nlp/stanford-corenlp/3.9.1/stanford-corenlp-3.9.1-models.jar
  * *
  * * spark/bin/spark-submit
  * *   --master [SPARK_MASTER]
  * *   --driver-memory [DRIVER_MEMORY]
  * *   --executor-memory [EXECUTOR_MEMORY]
  * *   --packages org.apache.hadoop:hadoop-aws:2.7.6,com.amazonaws:aws-java-sdk:1.7.4,databricks/spark-corenlp:0.3.1-s_2.11
  * *   --jars stanford-corenlp-3.9.1-models.jar
  * *   --conf spark.driver.extraClassPath=stanford-corenlp-3.9.1-models.jar
  * *   --conf spark.executor.extraClassPath=stanford-corenlp-3.9.1-models.jar
  * *   --class dpla.lda.Lemmas
  * *   [PATH_TO_JAR]
  * *   [INPUT]
  * *   [OUTPUT_DIRECTORY]
  **/
  *
  *
  *
  * To invoke via sbt:
  * sbt "run-main dpla.ingestion3.JsonlEntry
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
    val stopWords: String = cmdArgs.getStopWords
    val cvModel: String = cmdArgs.getCvModel
    val ldaModel: String = cmdArgs.getLdaModel
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
