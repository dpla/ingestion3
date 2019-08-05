package dpla.ingestion3.machineLearning

import com.databricks.spark.corenlp.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class Lemmatizer(spark: SparkSession) {

  // Flatten array of arrays
  private val flattenArrays: UserDefinedFunction =
    udf((arrays: collection.mutable.WrappedArray[collection.mutable.WrappedArray[String]]) => arrays.flatten)

  // Join strings in an array with ": "
  private val joinArray: UserDefinedFunction =
    udf((words: collection.mutable.WrappedArray[String]) => words.mkString(": "))

  /**
    * Lemmatize the text for specified columns of a DataFrame
    *
    * @param df DataFrame
    * @param inputCols Seq[String] names of columns to be lemmatized
    * @param outputCol String name of output column that will contain lemmas
    * @return DataFrame the input df with additional column containing lemmas
    *         Value of lemmas column may be null
    */
  def transform(df: DataFrame,
                inputCols: Seq[String],
                outputCol: String): DataFrame = {

    val columns: Seq[Column] = inputCols.map(x => col(x))

    val transformableDF = df.withColumn("localId", monotonically_increasing_id())

    val text: DataFrame = transformableDF.select(
      col("localId"),
      concat_ws(". ", columns:_*).as("text")
    )

    val lemmas = text
      .distinct // remove duplicates
      .withColumn("sentences", explode(ssplit(col("text"))))
      .filter(length(col("sentences")) > 0) // lemma method will throw exception if given empty string
      .withColumn("lem", lemma(col("sentences")))
      .select("localId", "lem")
      .groupBy("localId")
      .agg(collect_list(col("lem")).as("lemmas"))
      .select(col("localId"), flattenArrays(col("lemmas")).as(outputCol))

    transformableDF
      .join(lemmas, Seq("localId"), "left")
      .drop("localId")
  }
}
