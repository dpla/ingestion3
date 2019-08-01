package dpla.ingestion3.machineLearning

import org.apache.spark.sql.{DataFrame, SparkSession}


class TopicModelDriver(stopWordsSource: String,
                       cvModelSource: String,
                       ldaModelSource: String,
                       spark: SparkSession) {

  val lemmatizer = new Lemmatizer(spark)
  val bagOfWordsTokenizer = new BagOfWordsTokenizer(stopWordsSource, spark)
  val topicDistributor= new TopicDistributor(cvModelSource, ldaModelSource, spark)

  val dataFields: Seq[String] = Seq(
    "SourceResource.title",
    "SourceResource.subject.providedLabel",
    "SourceResource.description"
  )

  val idField: String = "dplaUri"

  // For parquet dumps:
  // idField = "doc.id"
  // dataFields =
  //    "doc.sourceResource.title",
  //    "doc.sourceResource.subject.name",
  //    "doc.sourceResource.description"

  def execute(enriched: DataFrame): DataFrame = {
    val lemmas: DataFrame =
      lemmatizer.transform(df=enriched, idCol=idField, inputCols=dataFields, outputCol="lemmas")

    val bagOfWords: DataFrame =
      bagOfWordsTokenizer.transform(df=lemmas, inputCol="lemmas", outputCol="bagOfWords")

    val topicDistributions: DataFrame =
      topicDistributor.transform(df=bagOfWords, idCol=idField, inputCol="bagOfWords", outputCol="topicDist")

    topicDistributions.select(idField, "lemmas", "bagOfWords", "topicDist")
  }
}
