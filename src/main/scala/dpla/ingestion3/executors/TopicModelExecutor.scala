package dpla.ingestion3.executors

import java.time.LocalDateTime

import com.databricks.spark.avro._
import dpla.ingestion3.dataStorage.OutputHelper
import dpla.ingestion3.machineLearning.{BagOfWordsTokenizer, Lemmatizer, TopicDistributor}
import dpla.ingestion3.messages._
import dpla.ingestion3.utils.Utils
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, concat_ws}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success}

trait TopicModelExecutor extends Serializable with IngestMessageTemplates {

  /**
    * Performs the mapping for the given provider
    *
    * @param sparkConf Spark configurations
    * @param dataIn Path to enriched data
    * @param dataOut Path to save topic model data
    * @param shortName Provider short name
    * @param stopWordsSource Path to stop words
    * @param cvModelSource Path to Count Vectorizer Model
    * @param ldaModelSource Path to LDA Model
    * @param logger Logger to use
    */
  def executeTopicModel(sparkConf: SparkConf,
                        dataIn: String,
                        dataOut: String,
                        shortName: String,
                        stopWordsSource: String,
                        cvModelSource: String,
                        ldaModelSource: String,
                        logger: Logger): String = {

    // The inputFileType should be avro or jsonl
    val inputFileType: String = dataIn.split("\\.").last.stripSuffix("/")

    // This start time is used for documentation and output file naming.
    val startDateTime = LocalDateTime.now

    // This start time is used to measure the duration of mapping.
    val startTime = System.currentTimeMillis

    // Output helper
    val outputHelper: OutputHelper =
      new OutputHelper(dataOut, shortName, "topic_model", startDateTime)

    val outputPath = outputHelper.activityPath

    // Initialize spark
    implicit val spark: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .config("spark.ui.showConsoleProgress", value = false)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") // Lemmatizer is very verbose

    // Initialize ML class instances
    val lemmatizer = new Lemmatizer(spark)
    val bagOfWordsTokenizer = new BagOfWordsTokenizer(stopWordsSource, spark)
    val topicDistributor= new TopicDistributor(cvModelSource, ldaModelSource, spark)

    // Read in enriched data and get text from relevant columns
    val rawText: DataFrame = inputFileType match {
      case "avro" => getAvroRawText(dataIn, spark)
      case "jsonl" => getJsonlRawText(dataIn, spark)
      case _ => throw new IllegalArgumentException("Input file type " + inputFileType + " not recognized.")
    }

    val lemmas: DataFrame =
      lemmatizer.transform(df=rawText, inputCol="text", outputCol="lemmas")

    val bagOfWords: DataFrame =
      bagOfWordsTokenizer.transform(df=lemmas, inputCol="lemmas", outputCol="bagOfWords")

    val topicDistributions: DataFrame =
      topicDistributor.transform(df=bagOfWords, inputCol="bagOfWords", outputCol="topicDist")

    // Write out topic distributions
    topicDistributions
      .select("dplaUri", "lemmas", "bagOfWords", "topicDist")
      .write
      .parquet(outputPath)

    val endTime: Double = System.currentTimeMillis
    val runTime: Double = endTime-startTime

    // Write manifest
    val manifestOpts: Map[String, String] = Map(
      "Activity" -> "Topic Model",
      "Provider" -> shortName,
      "Record count" -> rawText.count.toString,
      "Input" -> dataIn,
      "Runtime" -> Utils.formatRuntime(runTime.toLong)
    )
    outputHelper.writeManifest(manifestOpts) match {
      case Success(s) => logger.info(s"Manifest written to $s.")
      case Failure(f) => logger.warn(s"Manifest failed to write: $f")
    }

    logger.info("Finished in " + Utils.formatRuntime(runTime.toLong))

    spark.stop()

    // Return output destination of mapped records
    outputPath
  }

  def getAvroRawText(dataIn: String, spark: SparkSession): DataFrame = {

    // ID field of enriched records
    val idField: String = "dplaUri"

    // Fields of enriched records to use for topic modeling
    val dataFields: Seq[String] =
      Seq(
        "SourceResource.title",
        "SourceResource.subject.providedLabel",
        "SourceResource.description"
      )

    val dataCols: Seq[Column] = dataFields.map(x => col(x))

    val records: DataFrame = spark.read.avro(dataIn)

    // The column names must be "dplaUri" and "text"
    records.select(
      col(idField).as("dplaUri"), // this forces consistency between avro and jsonl models
      concat_ws(". ", dataCols:_*).as("text")
    )
  }

  /**
    * This can be depreciated once ingestion3 migration is complete and all hubs have authoritative enriched avro data
    * in dpla-master-dataset.
    */
  def getJsonlRawText(dataIn: String, spark: SparkSession): DataFrame = {
    import spark.sqlContext.implicits._

    val textRdd = spark.sparkContext.textFile(dataIn)

    val records: DataFrame = textRdd.map(text => {

      implicit val formats: DefaultFormats = DefaultFormats

      val json = parse(text).asInstanceOf[JObject]
      val id = (json \ "_source" \ "@id").extract[String]
      val sourceResource = json \ "_source" \ "sourceResource"

      val description: List[String] = sourceResource \ "description" match {
        case s: JString => List(s.extract[String])
        case a: JArray => a.extract[List[String]]
        case _ => List()
      }

      val title: List[String] = sourceResource \ "title" match {
        case s: JString => List(s.extract[String])
        case a: JArray => a.extract[List[String]]
        case _ => List()
      }

      val subject: List[String] = (sourceResource \ "subject")
        .extract[List[Map[String, String]]]
        .flatMap(_.get("name"))

      (id, description, title, subject)

    }).toDF("dplaUri", "description", "title", "subject")

    // The column names must be "dplaUri" and "text" for consistency with avro data
    records.select(
      col("dplaUri"),
      concat_ws(". ", col("description"), col("title"), col("subject")).as("text")
    )
  }
}
