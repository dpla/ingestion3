package dpla.ingestion3.machineLearning

import com.databricks.spark.corenlp.functions._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class BagOfWordsTokenizer(stopWordsSource: String, spark: SparkSession) extends Serializable {

  import spark.sqlContext.implicits._

  // These stopwords are altered during lemmatization.
  // They are not always altered in the records when the lemmatizer recognizes them as parts of proper nouns.
  // They must be added back into the stopwords list after lemmatization.
  private val forceStopWords: Seq[String] = Seq("archives", "collections", "libraries")

  // Lemmatized stop words
  private val stopWords: Seq[String] = spark.sparkContext
    .textFile(stopWordsSource) // read in files
    .toDF
    .select(explode(lemma(col("value")))) // lemmatize
    .map(_.getString(0))
    .collect
    .distinct ++: forceStopWords

  private val broadcastStopWords: Broadcast[Seq[String]] = spark.sparkContext.broadcast(stopWords)

  private val bagOfWords: UserDefinedFunction = udf(
    (words: collection.mutable.WrappedArray[String]) => {
      words
        .map(_.toLowerCase)
        .filter(_.forall(c => Character.isLetter(c))) // get only words comprised of letters
        .filter(!broadcastStopWords.value.contains(_))
        .filter(_.length > 1) // remove single-character words
    }
  )

  /**
    *
    * @param df DataFrame
    * @param inputCol String name of column containing lemmas
    * @param outputCol String name of output column that will contain bag-of-words tokens
    * @return Dataframe the original dataframe with an additional column containing bag-of-words tokens
    */
  def transform(df: DataFrame, inputCol: String, outputCol: String): DataFrame =
    df.withColumn(outputCol, bagOfWords(col(inputCol)))
}
