package dpla.ingestion3.entries.utils

import dpla.ingestion3.confs.CmdArgs
import dpla.ingestion3.dataStorage.InputHelper
import dpla.ingestion3.entries.Entry
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, explode, udf}

import scala.xml.{Elem, Node, XML}

object XmlSchemaAnalyzer {

  /** Recursively extracts all XPaths from an XML node
    * @param node
    *   The XML node to analyze
    * @param currentPath
    *   The current XPath being built
    * @return
    *   List of all XPaths found in the document
    */
  private def extractXPaths(
      node: Node,
      currentPath: String = ""
  ): List[String] = {
    val nodePath =
      if (currentPath.isEmpty) s"/${node.label}"
      else s"$currentPath/${node.label}"

    // Check if the node is a leaf (no child elements)
    val paths =
      if (node.child.isEmpty || node.child.forall(!_.isInstanceOf[Elem]))
        List(nodePath)
      else List.empty[String]

    // Recursively process child nodes
    val childPaths = node.child.flatMap {
      case child: Elem => extractXPaths(child, nodePath)
      case _           => List.empty[String]
    }

    paths ++ childPaths
  }

  /** Spark UDF that takes an XML string and returns a list of XPaths
    */
  private val extractXPathsUdf = udf((xmlString: String) => {
    try {
      val xml = XML.loadString(xmlString)
      extractXPaths(xml)
    } catch {
      case _: Exception => List.empty[String]
    }
  })

  /** Analyzes a DataFrame containing XML strings and returns a DataFrame with
    * XPath counts
    * @param df
    *   Input DataFrame containing XML strings in the specified column
    * @param xmlColumn
    *   Name of the column containing XML strings
    * @return
    *   DataFrame with columns: xpath (String) and count (Long)
    */
  def analyzeXmlPaths(df: DataFrame, xmlColumn: String): DataFrame = {
    df.select(explode(extractXPathsUdf(col(xmlColumn))).alias("xpath"))
      .groupBy("xpath")
      .agg(count("*").alias("count"))
      .orderBy(col("xpath").desc)
  }

  /** Main function to demonstrate usage
    */
  def main(args: Array[String]): Unit = {

    Entry.suppressUnsafeWarnings()

    val input = args(0)

    val harvestData =
      if (InputHelper.isActivityPath(input)) input
      else
        InputHelper
          .mostRecent(input)
          .getOrElse(throw new RuntimeException("Unable to load harvest data."))

    val spark = SparkSession
      .builder()
      .master("local[6]")
      .appName("XML Schema Analyzer")
      .getOrCreate()

    try {
      val df = spark.read.format("avro").load(harvestData)
      val result = analyzeXmlPaths(df, "document")

      // Print results
      println("\nXPath Analysis Results:")
      println("=====================")
      result.collect().foreach(println)

    } finally {
      spark.stop()
    }
  }
}
