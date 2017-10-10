package dpla.ingestion3.premappingreports

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

import scala.util.parsing.json.{JSON, JSONType}

class PreMappingReport(val input: DataFrame,
                       val inputDataType: String,
                       val spark: SparkSession) extends Serializable {

  /**
    * Basis for all other calculations.
    * The return dataframe has columns "id", "attribute", "value".
    * The "id" is the id of the record.
    * A single record is represented by many rows.
    */
  lazy val tripleShreds: DataFrame = {
    val dataframe: DataFrame = inputDataType match {
      case "xml" => XmlShredder.process(input)
      case "json" => JsonShredder.process(input)
      case _ =>
        throw new RuntimeException(s"Unknown input data type: $inputDataType")
    }
    // Persist so that this doesn't have to be recalculated for each calculation.
    dataframe.persist(StorageLevel.DISK_ONLY)
    // Return dataframe.
    dataframe
  }

  /**
    * The return dataframe has columns for "id" and each attribute.
    * One row represents one record.
    * The value of each cell is a Array containing all the record's
    * value for the attribute.  If a record has no occurrences of an attribute,
    * the value will be an empty Array.
    *
    * @return DataFrame
    */
  lazy val aggregateValues: DataFrame =
    tripleShreds.groupBy("id")
      .pivot("attribute")
      .agg(collect_list(col("value")))

  /**
    * The return dataframe has columns for "id" and each attribute.
    * One row represents one record.
    * The value of each cell is and Integer representing the number of
    * occurrences of each attribute in the record.  If a record has no
    * occurrences of an attribute, the value will be null.
    *
    * @return DataFrame
    */
  lazy val cardinality: DataFrame =
    tripleShreds.groupBy("id")
      .pivot("attribute")
      .agg(count(col("value")))

  /**
    * The return dataframe has columns for "id" and for each attribute.
    * One row represents one record.
    * Multiple values for an attribute are concatenated into a single string and
    * separated with " | ".
    * This dataframe is formatted to work with Google Refine in the QA workflow.
    *
    * @return DataFrame
    */
  lazy val openRefineData: DataFrame =
    tripleShreds.groupBy("id")
      .pivot("attribute")
      .agg(concat_ws(" | ", collect_list(col("value"))))

  lazy val elasticSearchData: RDD[JSONType] = {
    // Change each row to a JSON String.
    val json: Dataset[String] = aggregateValues.toJSON

    // Convert Dataset to RDD.  Datasets run into encoder errors when you try to
    // parse their Rows to JSON.
    json.rdd.flatMap(x => {
      // Create proper nested JSON format to work with elasticdump.
      val string = """{"_source": """ + x + """}"""
      // Convert Strings into JSONType objects so they can be written out to
      // JSON files.
      JSON.parseRaw(string)
    })
  }

  /**
    * The return dataframe has columns "attribute" and "recordsMissingAttribute".
    * The values indicate the total number of records that do NOT have a value
    * for the attribute.
    *
    * @return DataFrame
    */
  lazy val recordsMissingAttributes: DataFrame = {
    // Distributed list of all the attributes.
    // The `drop(1)' command removes the DPLA record id.
    val columns: Array[String] = cardinality.columns.drop(1)

    // Get the total number of records that are missing for each attribute.
    val missing: Array[missingAttr] = columns.map(attr => {
      // In the `select' command, encase the name of the attribute in backticks.
      // This ensures that any special characters will be escaped.
      val recordsMissingAttr = cardinality.select(s"`$attr`").rdd.map(x => {
        if (x(0) == null) 1 else 0
      }).reduce(_+_)
      missingAttr(attr, recordsMissingAttr)
    })

    spark.sqlContext.createDataFrame(missing)
  }
}

/**
  * @see recordsMissingAttributes
  */
case class missingAttr(attribute: String, recordsMissingAttribute: Int)
