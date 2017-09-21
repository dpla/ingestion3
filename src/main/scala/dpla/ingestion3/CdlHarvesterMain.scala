package dpla.ingestion3

import java.io.File

import dpla.ingestion3.confs.{CmdArgs, Ingestion3Conf}
import dpla.ingestion3.harvesters.api._
import dpla.ingestion3.harvesters.file.NaraFileHarvestMain.getAvroWriter
import dpla.ingestion3.utils.{FlatFileIO, Utils}
import org.apache.avro.Schema
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericRecord
import org.apache.log4j.LogManager
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._


object CdlHarvesterMain extends ApiHarvester {

  private val logger = LogManager.getLogger(getClass)

  /**
    * Driver for harvesting from California Digital Library's Solr endpoint
    * (https://solr.calisphere.org/solr/query).
    *
    * Expects command line args of
    * --apiKey - The CDL api key
    * --rows - Number of records to return per page. Defaults to 10
    * --outputFile - Location to save the avro. Defaults to ./out
    * --query - Solr query to select the records. Defaults to *:*
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // Parse and set the command line options
    val cmdArgs = new CmdArgs(args)

    val outputDir = cmdArgs.output.toOption
      .map(new File(_))
      .getOrElse(throw new RuntimeException("No output specified"))
    val confFile = cmdArgs.configFile.toOption
      .map(_.toString)
      .getOrElse(throw new RuntimeException("No conf file specified"))
    val providerName = cmdArgs.providerName.toOption
      .map(_.toString)
      .getOrElse(throw new RuntimeException("No provider name specified"))

    val i3Conf = new Ingestion3Conf(confFile, Some(providerName))
    val providerConf = i3Conf.load()

    val queryParams: Map[String, String] = Map(
      "query" -> providerConf.harvest.query,
      "rows" -> providerConf.harvest.rows,
      "api_key" -> providerConf.harvest.apiKey
    ).collect{ case (key, Some(value)) => key -> value } // remove None values

    println(queryParams)
    // Must do this before setting the avroWriter
    outputDir.getParentFile.mkdir()
    Utils.deleteRecursively(outputDir)

    val schemaStr = new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc")
    val schema = new Schema.Parser().parse(schemaStr)
    val avroWriter = getAvroWriter(outputDir, schema)

    startHarvest(outputDir, queryParams, avroWriter, schema)
  }

  /**
    *
    * @param queryParams Map of query parameters
    * @param avroWriter Writer
    * @param schema Schema to apply to data
    */
  override def doHarvest(queryParams: Map[String, String],
                         avroWriter: DataFileWriter[GenericRecord],
                         schema: Schema): Unit = {
    implicit val formats = DefaultFormats

    val cdl = new CdlHarvester(queryParams)

    // Mutable vars for controlling harvest loop
    var continueHarvest = true
    var cursorMark = "*"

    // Runtime tracking
    val startTime = System.currentTimeMillis()

    while(continueHarvest) cdl.harvest(cursorMark) match {
      case error: ApiError with ApiResponse =>
        logger.error("Error returned by request %s\n%s\n%s".format(
          error.errorSource.url.getOrElse("Undefined url"),
          error.errorSource.queryParams,
          error.message
        ))
        continueHarvest = false
      case src: ApiSource with ApiResponse =>
        src.text match {
          case Some(docs) =>
            val json = parse(docs)
            val cdlRecords = (json \\ "docs").children.map(doc => {
              ApiRecord((doc \\ "identifier").toString, compact(render(doc)))
            })

            saveOut(avroWriter, cdlRecords, schema, "cdl", "application_json")

            // Loop control
            cursorMark = (json \\ "cursorMark").extract[String]
            val nextCursorMark = (json \\ "nextCursorMark").extract[String]

            cursorMark.matches(nextCursorMark) match {
              case true => continueHarvest = false
              case false => cursorMark = nextCursorMark
            }
          case None =>
            logger.error(s"The body of the response is empty. Stopping run.\nCdlSource >> ${src.toString}")
            continueHarvest = false
        }
      case _ =>
        logger.error("Harvest returned None")
        continueHarvest = false
    }
  }
}
