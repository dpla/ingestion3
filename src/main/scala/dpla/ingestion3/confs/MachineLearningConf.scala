package dpla.ingestion3.confs

import com.typesafe.config.{ConfigFactory,Config}

object MachineLearningConf {

  // Load settings from application.conf
  val configs: Config = ConfigFactory.load("machineLearning")

  val stopWordsPath: String = configs.getString("lda.stopWordsPath")
  val ldaModelPath: String = configs.getString("lda.ldaModelPath")
  val cvModelPath: String = configs.getString("lda.cvModelPath")

}
