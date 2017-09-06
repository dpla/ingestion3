package dpla.ingestion3

import java.io.File

import dpla.ingestion3.harvesters.api._
import dpla.ingestion3.harvesters.file.NaraFileHarvestMain.getAvroWriter
import dpla.ingestion3.utils.{FlatFileIO, Utils}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.log4j.LogManager
import org.apache.spark
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop.{ScallopConf, ScallopOption}


object CdlHarvesterMain {

  private val logger = LogManager.getLogger(getClass)

  /**
    * Driver for harvesting from California Digital Library's Solr endpoint
    * (https://solr.calisphere.org/solr/query).
    *
    * Expects command line args of
    * --endpoint=solr.calisphere.org.
    * --apiKey - The CDL api key
    * --rows - Number of records to return per page. Defaults to 10
    * --outputFile - Location to save the avro. Defaults to ./out
    * --query - Solr query to select the records. Defaults to *:*
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // Parse and set the command line options
    val conf = new CdlHarvestConf(args)
    val outFile = new File(conf.outputFile.getOrElse("out"))
    // Will never get here b/c if the API key isn't
    // provided this will blow up Scallop validation
    val apiKey = conf.apiKey.getOrElse("MISSING API KEY")
    val query = conf.query.getOrElse("*:*")
    val rows = conf.rows.getOrElse("100")
    // Same as apiKey, this is validated at invocation.
    val endpoint = conf.endpoint.getOrElse("MISSING ENDPOINT")

    implicit val formats = DefaultFormats

    Utils.deleteRecursively(outFile)

    val schemaStr = new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc")
    val schema = new Schema.Parser().parse(schemaStr)
    val avroWriter = getAvroWriter(outFile, schema)

    val queryParams = Map(
      "query" -> query,
      "rows" -> rows,
      "api_key" -> apiKey
    )

    val cdl = new CdlHarvester(endpoint, queryParams)

    // Mutable vars for controlling harvest loop
    var doHarvest = true
    var cursorMark = "*"

    // Runtime tracking
    val startTime = System.currentTimeMillis()

    while(doHarvest) cdl.harvest(cursorMark) match {
      case error: CdlError with CdlResponse =>
        logger.error("Error returned by request %s\n%s\n%s".format(
          error.errorSource.url.getOrElse("Undefined url"),
          error.errorSource.queryParams,
          error.message
        ))
        doHarvest = false
      case src: CdlSource with CdlResponse =>
        src.text match {
          case Some(docs) =>
            val json = parse(docs)
            // Transforms the CdlSource result into List[CdlRecord]. Creating a CdlRecord
            // requires extracting the record identifier.
            val cdlRecords = (json \\ "docs").children.map(doc => {
              CdlRecord((doc \\ "identifier").toString, compact(render(doc)))
            })

            // After transforming all records they are shipped to be saved.
            saveOut(avroWriter,
              cdlRecords,
              outFile,
              schema)

            // Compare current and next cursor values to determine whether the harvest is over.
            // println(pretty(json))
            cursorMark = (json \\ "cursorMark").extract[String]
            val nextCursorMark = (json \\ "nextCursorMark").extract[String]

            cursorMark.matches(nextCursorMark) match {
              case true => doHarvest = false
              case false => cursorMark = nextCursorMark
            }
          case None =>
            logger.error(s"The body of the response is empty. Stopping run.\nCdlSource >> ${src.toString}")
            doHarvest = false
        }
      case _ =>
        logger.error("Harvest returned None")
        doHarvest = false
     }

    val endTime = System.currentTimeMillis()

    import com.databricks.spark.avro._
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder().master("local").getOrCreate()
    val recordCount = spark.read.avro(outFile.toString).count()

    Utils.printResults(endTime-startTime, recordCount)
  }

  /**
    * Saves the records
    *
    * @param avroWriter
    * @param docs
    * @param outFile
    * @param schema
    */
  def saveOut(avroWriter: DataFileWriter[GenericRecord], docs: List[CdlRecord], outFile: File, schema: Schema): Unit = {
    docs.foreach(doc => {
      val startTime = System.currentTimeMillis()
      val unixEpoch = startTime / 1000L
      val genericRecord = new GenericData.Record(schema)

      genericRecord.put("id", doc.id)
      genericRecord.put("ingestDate", unixEpoch)
      genericRecord.put("provider", "CDL")
      genericRecord.put("document", doc.document)
      genericRecord.put("mimetype", "application_json")
      avroWriter.append(genericRecord)
    })
  }
}

/**
  * CDL harvester command line parameters
  *
  * @param arguments
  */
class CdlHarvestConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val outputFile: ScallopOption[String] = opt[String](
    "outputFile",
    required = true,
    noshort = true,
    validate = _.endsWith(".avro"),
    descr = "Output file must end with .avro"
  )

  val apiKey: ScallopOption[String] = opt[String](
    "apiKey",
    required = true,
    noshort = true,
    validate = _.nonEmpty
  )

  val endpoint: ScallopOption[String] = opt[String](
    "endpoint",
    required = true,
    noshort = true,
    validate = _.nonEmpty
  )

  val query: ScallopOption[String] = opt[String](
    "query",
    required = false,
    noshort = true,
    validate = _.nonEmpty
  )

  val rows: ScallopOption[String] = opt[String](
    "rows",
    required = false,
    noshort = true,
    validate = _.nonEmpty
  )

  verify()
}