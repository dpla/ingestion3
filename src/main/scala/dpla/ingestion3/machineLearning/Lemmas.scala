package dpla.ingestion3.machineLearning

import com.databricks.spark.corenlp.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class Lemmas(spark: SparkSession) {

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
    * @param idCol String name of column to be used as unique identifier
    * @param inputCols Seq[String] names of columns to be lemmatized
    * @param outputCol String name of output column (lemmas)
    * @return DataFrame the input df with additional column of lemmas
    *         Value of lemmas column may be null
    */
  def transform(df: DataFrame,
                idCol: String,
                inputCols: Seq[String],
                outputCol: String): DataFrame = {

    val columns: Seq[Column] = inputCols.map(x => col(x))

    val text: DataFrame = df.select(
      col(idCol).as("id"),
      concat_ws(". ", columns:_*).as("text")
    )

    val lemmas = text
      .distinct // remove duplicates
      .withColumn("sentences", explode(ssplit(col("text"))))
      .filter(length(col("sentences")) > 0) // lemma method will throw exception if given empty string
      .withColumn("lem", lemma(col("sentences")))
      .select("id", "lem")
      .groupBy("id")
      .agg(collect_list(col("lem")).as("lemmas"))
      .select(col("id"), flattenArrays(col("lemmas")).as(outputCol))

    df.join(lemmas, col(idCol) === col("id"), "left")
  }
}
