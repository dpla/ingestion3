package dpla.ingestion3.executors

import java.time.LocalDateTime

import com.databricks.spark.avro._
import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.model.{ModelConverter, getDplaId, wikiRecord}
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success}

case class WikiMetadata(dplaId: String, wikiMarkup: String)

trait WikiMetadataExecutor extends Serializable {

  /**
    * Generate Wiki metadata JSON files from AVRO file
    * @param sparkConf  Spark configuration
    * @param dataIn     Data to transform into Wiki metadata
    * @param dataOut    Location to save Wikimedia metadata
    * @param shortName  Provider shortname
    * @param logger     Logger object
    */
  def executeWikiMetadata(sparkConf: SparkConf,
                   dataIn: String,
                   dataOut: String,
                   shortName: String,
                   logger: Logger): String = {

    // This start time is used for documentation and output file naming.
    val startDateTime: LocalDateTime = LocalDateTime.now

    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "wiki", startDateTime)

    val outputPath: String = outputHelper.activityPath

    logger.info("Starting Wiki export")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val enrichedRows: DataFrame = spark.read.avro(dataIn)

    // TODO filter out non-eligible enrichedRows

    enrichedRows.foreach(
      row => {
        val record = ModelConverter.toModel(row)
        val dplaId = getDplaId(record)
        val wikiMarkup = wikiRecord(record)
        val path = getWikiPath(dplaId)
        println(
          s"""
            | Output path: $path
             Output file: ${}
             Wikimarkup: ${wikiMarkup}
          """.stripMargin)

        // TODO write files out
      }
    )

    // val wikiCount = wikiRecords.count

    // This should always write out as #text() because if we use #json() then the
    // data will be written out inside a JSON object (e.g. {'value': <doc>}) which is
    // invalid for our use

    // FIXME do not write out as text files need to
    // indexRecords.write.text(outputPath)
    // wikiRecords.toDF()

    // println(wikiRecords.count())

    //    wikiRecords.toDF.map(
//      wikiRecord => {

//      }
//    )

    // Create and write manifest.

    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "Wiki",
      "Provider" -> shortName,
      "Record count" -> "TBD",
      "Input" -> dataIn
    )

    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    sc.stop()

    logger.info("Wiki export complete")

    // Return output path of wiki files.
    outputPath
  }

  /**
    *
    * @param str
    * @return
    */
  def getWikiPath(str: String) = {
    s"{${str(0)}/${str(1)}/${str(2)}/${str(3)}/$str/}"
  }


}

