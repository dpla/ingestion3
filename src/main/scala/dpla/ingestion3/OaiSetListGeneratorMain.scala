package dpla.ingestion3

import com.databricks.spark.avro._
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Entry point for generating an list of OAI sets.
  * The set list can be input for OaiRecordHarvesterMain
  *
  * args Input directory: String
  */
object OaiSetListGeneratorMain {

  val logger = LogManager.getLogger(OaiSetListGeneratorMain.getClass)

  def main(args: Array[String]): Unit = {

    validateArgs(args)

    val inputFile = args(0)

    // Initiate spark session.
    val sparkConf = new SparkConf().setAppName("Oai Set List Generator")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    val dataFrame = spark.read.avro(inputFile)

    // Convert the ID column into an Array of IDs.
    val setArray = dataFrame.select("id").rdd.map(r => r.getString(0)).collect

    // Convert Array to comma-separated String.
    val string = setArray.mkString(",")

    println(string)

    // Stop spark session
    sc.stop()
  }

  def validateArgs(args: Array[String]) = {
    // Complains about not being typesafe...
    if(args.length != 1) {
      logger.error("Bad number of arguments passed to OAI harvester. Expecting:\n" +
        "\t<INPUT AVRO FILE>\n")
    }
  }
}
