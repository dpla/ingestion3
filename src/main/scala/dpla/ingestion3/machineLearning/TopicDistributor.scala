package dpla.ingestion3.machineLearning

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.mllib.clustering.LocalLDAModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._


class TopicDistributor(cvModelSource: String,
                       ldaModelSource: String,
                       spark: SparkSession) {

//  private lazy val cvModel: CountVectorizerModel = CountVectorizerModel.read.load(cvModelSource)
//  private lazy val ldaModel: LocalLDAModel = LocalLDAModel.load(spark.sparkContext, ldaModelSource)
//  private lazy val localLdaModel: LocalLDAModel = ldaModel.asInstanceOf[LocalLDAModel] // TODO: Necessary?

  /**
    *
    * @param df DataFrame
    * @param inputCol String name of column containing bag-of-words tokens
    * @param outputCol String name of output column that will contain topic distributions
    * @return DataFrame the original Dataframe with additional column containing topic distributions
    */
  def transform(df: DataFrame,
                inputCol: String,
                outputCol: String): DataFrame = {

    val cvModel: CountVectorizerModel = CountVectorizerModel.read.load(cvModelSource)
    val ldaModel: LocalLDAModel = LocalLDAModel.load(spark.sparkContext, ldaModelSource)
    val localLdaModel: LocalLDAModel = ldaModel.asInstanceOf[LocalLDAModel] // TODO: Necessary?

    // localLDAModel requires a numeric ID
    val transformableDF: DataFrame = df.withColumn("numericId", monotonically_increasing_id)

    val wordVectors: RDD[(Long, Vector)] = cvModel
      .setInputCol(inputCol)
      .transform(transformableDF)
      .select("numericId", "termFreq")
      .rdd
      .map { case Row(id: Long, termFreq: MLVector) => (id, Vectors.fromML(termFreq)) }
      .persist(StorageLevel.MEMORY_AND_DISK_SER) // avoid multiple re-evaluations when calculating distributions

    import spark.sqlContext.implicits._

    val distributions: DataFrame = localLdaModel
      .topicDistributions(wordVectors) // get distributions
      .map{ case(id, vector) => (id, vector.toArray) } // convert Vector to Array[Double]
      .toDF("numericId", outputCol)

    transformableDF
      .join(distributions, Seq("numericId"), joinType="left")
      .drop("numericId")
  }
}
