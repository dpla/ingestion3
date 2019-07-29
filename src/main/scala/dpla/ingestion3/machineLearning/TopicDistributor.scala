package dpla.ingestion3.machineLearning

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.mllib.clustering.LocalLDAModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._


class TopicDistributor(cvModelSource: String,
                       ldaModelSource: String,
                       spark: SparkSession) {

  import spark.sqlContext.implicits._

  private lazy val cvModel: CountVectorizerModel = CountVectorizerModel.read.load(cvModelSource)
  private lazy val ldaModel: LocalLDAModel = LocalLDAModel.load(spark.sparkContext, ldaModelSource)
  private lazy val localLdaModel: LocalLDAModel = ldaModel.asInstanceOf[LocalLDAModel] // TODO: Necessary?

  /**
    *
    * @param df DataFrame
    * @param idCol String name of column containing identifier
    * @param inputCol String name of column containing bag-of-words tokens
    * @param outputCol String name of output column that will contain topic distributions
    * @return DataFrame the original Dataframe with additional column containing topic distributions
    */
  def transform(df: DataFrame,
                idCol: String,
                inputCol: String,
                outputCol: String): DataFrame = {

    val wordVectors = cvModel
      .transform(df)
      .select(idCol, "termFreq")
      .rdd
      .map { case Row(id: Long, termFreq: MLVector) => (id, Vectors.fromML(termFreq)) }
      .persist(StorageLevel.MEMORY_AND_DISK_SER) // avoid multiple re-evaluations when calculating distributions

    val distributions: DataFrame = localLdaModel
      .topicDistributions(wordVectors) // get distributions
      .map{ case(id, vector) => (id, vector.toArray) } // convert Vector to Array[Double]
      .toDF("id", outputCol)

    df.join(distributions, col(idCol) === col("id"), joinType="left")
  }
}
