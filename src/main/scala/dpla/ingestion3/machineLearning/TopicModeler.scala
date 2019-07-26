package dpla.ingestion3.enrichments.lda

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.mllib.clustering.LocalLDAModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel



class TopicModeler(dplaMapData: DataFrame,
                    cvModelSource: String,
                    ldaModelSource: String,
                    stopWordsSource: String,
                    spark: SparkSession) {

  import spark.sqlContext.implicits._

  private lazy val cvModel: CountVectorizerModel = CountVectorizerModel.read.load(cvModelSource)
  private lazy val ldaModel: LocalLDAModel = LocalLDAModel.load(spark.sparkContext, ldaModelSource)
  private lazy val localLdaModel: LocalLDAModel = ldaModel.asInstanceOf[LocalLDAModel] // TODO: Necessary?
  private lazy val rawStopWords: RDD[String] = spark.sparkContext.textFile(stopWordsSource)

  // These stopwords are altered during lemmatization.
  // They are not always altered in the records when the lemmatizer recognizes them as parts of proper nouns.
  // They must be added back into the stopwords list after lemmatization.
  val forceStopWords: Seq[String] = Seq("archives", "collections", "libraries")

  private lazy val stopWords: Seq[String] = rawStopWords
    .toDF
    .select(explode(lemma(col("value"))))
    .map(_.getString(0))
    .collect
    .distinct ++: forceStopWords


  def bagOfWords(lemmas: DataFrame) = {



  }

  private def stopWords(le: Seq[String] =

  def wordVectors(words: DataFrame): RDD[(Long, Vector)] =
    cvModel.transform(words)
      .select("uniqueId", "termFreq")
      .rdd
      .map { case Row(id: Long, termFreq: MLVector) => (id, Vectors.fromML(termFreq)) }
  //      .persist(StorageLevel.MEMORY_AND_DISK_SER)

  def distributions(vectors: RDD[(Long, Vector)]): RDD[(Long, Vector)] =
    localLdaModel.topicDistributions(vectors)


}
