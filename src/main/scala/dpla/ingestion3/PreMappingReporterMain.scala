package dpla.ingestion3

import dpla.ingestion3.premappingreports._
import org.apache.log4j.{LogManager, Logger}

import scala.util.Failure

/**
  * PreMappingReporterMain and PreMappingReporter, for generating QA reports
  * on harvested documents.
  *
  * Example invocation:
  *
  *   $ sbt "run-main dpla.ingestion3.PreMappingReporterMain \
  *     /path/to/harvested-data.avro /path/to/pre-mapping-report local[2] xml
 */

/**
  * PreMappingReporter, the driver class.
  *
  * The design patters for PreMappingReporter are the same as those for Reporter.
  *
  * @param inputURI         Input URI or file path
  * @param outputURI        Output URI or file path
  * @param sparkMasterName  Spark master name, e.g. "local[1]"
  * @param inputDataType    The data type of the input data ("xml", "json")
  */



/**
  * Entry point for running a pre-mapping report.
  */
object PreMappingReporterMain {

  def usage(): Unit = {
    println(
      """
        |Usage:
        |
        |PreMappingReporterMain <input> <output> <spark master> <input data type>
      """.stripMargin)
  }

  val logger: Logger = LogManager.getLogger("PreMappingReporter")

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      usage()
      System.err.println("Incorrect invocation arguments")
      sys.exit(1)
    }
    val input = args(0)
    val output = args(1)
    val sparkMasterName = args(2)
    val inputDataType = args(3)

    val result = new PreMappingReporter(input, output, sparkMasterName, inputDataType)
      .writeAllReports

    result match {
      case Failure(e) => logger.error(e.toString)
      case _ => Unit
    }
  }
}
