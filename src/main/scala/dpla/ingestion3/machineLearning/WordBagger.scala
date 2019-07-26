package dpla.ingestion3.machineLearning

import com.databricks.spark.corenlp.functions._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

trait WordBagger {

  val spark: SparkSession

  import spark.sqlContext.implicits._

  private val stopWordsSources: Seq[String] = Seq(
    "/stopwords/core-nlp-stopwords.txt",
    "/dpla-stopwords.txt"
  )

  // These stopwords are altered during lemmatization.
  // They are not always altered in the records when the lemmatizer recognizes them as parts of proper nouns.
  // They must be added back into the stopwords list after lemmatization.
  private val forceStopWords: Seq[String] = Seq("archives", "collections", "libraries")

  // Complete list of lemmatized stop words
  private lazy val stopWords: Seq[String] = stopWordsSources
    .map(fileName => spark.sparkContext.textFile(fileName)) // read in files
    .reduce{ (x,y) => x ++ y } // combine into single RDD
    .toDF
    .select(explode(lemma(col("value")))) // lemmatize
    .map(_.getString(0))
    .collect
    .distinct ++: forceStopWords


  lazy val broadcastStopWords: Broadcast[Seq[String]] = spark.sparkContext.broadcast(stopWords)

  // UDFs

  val cleanUpLemmas: UserDefinedFunction = udf(
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
    * @param df Dataframe
    * @param inputCol String name of column containing lemmas
    * @param outputCol String name of output column that will contain bag-of-words tokens
    * @return Dataframe the original with an additional column containing bag-of-words tokens
    */
  def bagOfWords(df: DataFrame, inputCol: String, outputCol: String): DataFrame =
    df.withColumn(outputCol, cleanUpLemmas(col(inputCol)))
}
