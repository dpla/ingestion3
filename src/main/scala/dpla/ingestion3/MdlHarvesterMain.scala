package dpla.ingestion3

import java.io.File

import dpla.ingestion3.harvesters.api._
import dpla.ingestion3.harvesters.file.NaraFileHarvestMain.getAvroWriter
import dpla.ingestion3.utils.{FlatFileIO, Utils}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.log4j.LogManager
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.rogach.scallop.{ScallopConf, ScallopOption}


object MdlHarvesterMain extends ApiHarvester {

  private val logger = LogManager.getLogger(getClass)

  /**
    * Driver for harvesting from Minnesota Digital Library's Solr endpoint
    *
    * Expects command line args of
    * --rows - Number of records to return per page. Defaults to 10
    * --outputFile - Location to save the avro. Defaults to ./out
    * --query - The query to select the records. Defaults to *:*
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    // Parse and set the command line options
    val conf = new MdlHarvestConf(args)
    val outFile = new File(conf.outputFile.getOrElse("out"))
    val query = conf.query.getOrElse("*:*")
    val rows = conf.rows.getOrElse("10")

    val queryParams = Map(
      "query" -> query,
      "rows" -> rows
    )
    // Must do this before setting the avroWriter
    outFile.getParentFile.mkdir()
    Utils.deleteRecursively(outFile)

    val schemaStr = new FlatFileIO().readFileAsString("/avro/OriginalRecord.avsc")
    val schema = new Schema.Parser().parse(schemaStr)
    val avroWriter = getAvroWriter(outFile, schema)

    startHarvest(outFile, queryParams, avroWriter, schema)
  }

  override def doHarvest(queryParams: Map[String,String],
                         avroWriter: DataFileWriter[GenericRecord],
                         schema: Schema): Unit = {
    implicit val formats = DefaultFormats

    val mdl = new MdlHarvester(queryParams)

    // Mutable vars for controlling harvest loop
    var continueHarvest = true
    var start = 0

    while(continueHarvest) mdl.harvest(start.toString) match {
      case error: ApiError with ApiResponse =>
        println("Error returned by request %s\n%s\n%s".format(
          error.errorSource.url.getOrElse("Undefined url"),
          error.errorSource.queryParams,
          error.message
        ))
        continueHarvest = false
      case src: ApiSource with ApiResponse =>
        src.text match {
          case Some(docs) =>
            val json = parse(docs)

            println(s"Requesting $start of ${(json \\ "numFound").extract[String]}")

            val mdlRecords = (json \\ "docs").children.map(doc => {
              ApiRecord((doc \\ "record_id").toString, compact(render(doc)))
            })

            saveOut(avroWriter, mdlRecords, schema, "mdl", "application_json")

            // Number of records returned < number of records requested
            val rows = queryParams.getOrElse("rows", "10").toInt
            mdlRecords.size < rows match {
              case true => continueHarvest = false
              case false => start += rows
            }
          case None =>
            logger.error(s"The body of the response is empty. Stopping run.\nApiSource >> ${src.toString}")
            continueHarvest = false
        }
      case _ =>
        logger.error("Harvest returned None")
        continueHarvest = false
    }


  }
}

/**
  * MDL harvester command line parameters
  *
  * @param arguments
  */
class MdlHarvestConf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val outputFile: ScallopOption[String] = opt[String](
    "outputFile",
    required = true,
    noshort = true,
    validate = _.endsWith(".avro"),
    descr = "Output file must end with .avro"
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