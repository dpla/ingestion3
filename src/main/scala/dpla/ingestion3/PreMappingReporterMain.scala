package dpla.ingestion3

import dpla.ingestion3.premappingreports._
import org.apache.log4j.{LogManager, Logger}
import scala.util.{Failure, Try}

/**
  * PreMappingReporterMain and PreMappingReporter, for generating QA reports
  * on harvested documents.
  *
  * Example invocation:
  *
  *   $ sbt "run-main dpla.ingestion3.PreMappingReporterMain \
  *     /path/to/harvested-data.avro /path/to/pre-mapping-report local[2] \
  *     xmlShredder"
 */

/**
  * PreMappingReporter, the driver class.
  *
  * The design patters for PreMappingReporter are the same as those for Reporter.
  *
  * @param inputURI         Input URI or file path
  * @param outputURI        Output URI or file path
  * @param sparkMasterName  Spark master name, e.g. "local[1]"
  * @param token            The report name token
  */
class PreMappingReporter (token: String,
                          inputURI: String,
                          outputURI: String,
                          sparkMasterName: String) {

  val logger: Logger = LogManager.getLogger(this.getClass)

  private def getPreMappingReport(token: String): Option[PreMappingReport] = {
    token match {
      case "xmlShredder" =>
        Some(new XmlShredderReport(inputURI, outputURI, sparkMasterName))
      case _ => None
    }
  }

  def main(): Unit = {
    val result: Try[Unit] = getPreMappingReport(token) match {
      case Some(shreds) => shreds.run()
      case None => Failure(
        new RuntimeException(s"Pre-mapping report type $token is unknown")
      )
    }
    result match {
      case Failure(ex) =>
        logger.error(ex.toString)
        logger.error("\n" + ex.getStackTrace.mkString("\n"))
      case _ => Unit
    }
  }
}

/**
  * Entry point for running a pre-mapping report.
  */
object PreMappingReporterMain {

  def usage(): Unit = {
    println(
      """
        |Usage:
        |
        |PreMappingReporterMain <input> <output> <spark master> <report token>
      """.stripMargin)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      usage()
      System.err.println("Incorrect invocation arguments")
      sys.exit(1)
    }
    val inputURI = args(0)
    val outputURI = args(1)
    val sparkMasterName = args(2)
    val token = args(3)

    new PreMappingReporter(
      token, inputURI, outputURI, sparkMasterName
    ).main()
  }
}

