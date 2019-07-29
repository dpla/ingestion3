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

    val logger = Utils.createLogger("jsonl")

    executeTopicModel(sparkConf, dataIn, dataOut, shortName, stopWords, cvModel, ldaModel, logger)
  }
}
