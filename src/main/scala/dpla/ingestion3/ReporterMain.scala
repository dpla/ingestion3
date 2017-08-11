package dpla.ingestion3

import org.apache.log4j.{Logger, LogManager}
import dpla.ingestion3.reports._
import util.{Try, Failure}


/*
 * ReporterMain and Reporter, for generating QA reports
 *
 * Example invocation:
 *
  *   $ sbt "run-main dpla.ingestion3.ReporterMain \
  *     /path/to/enriched-data.avro /path/to/propvalreport local[2] \
  *     propertyValue sourceResource.type"
 */


/**
  * Reporter, the report driver class.
  *
  * This is a class that is instantiated by ReporterMain because the original
  * idea was it would be possible to unit test this way.  Note that getReport()
  * is probably the only method that is worth testing.  See the unit tests in
  * ReporterTest.
  *
  * @see ReporterTest
  *
  *
  * @param inputURI         Input URI or file path
  * @param outputURI        Output URI or file path
  * @param sparkMasterName  Spark master name, e.g. "local[1]"
  * @param token            The report name token
  * @param reportParams     Additional parameters particular to the report
  */
class Reporter (
                 token: String,
                 inputURI: String,
                 outputURI: String,
                 sparkMasterName: String,
                 reportParams: Array[String]
               ) {

  val logger: Logger = LogManager.getLogger(this.getClass)

  private def getReport(token: String): Option[Report] = {
    token match {
      case "propertyDistinctValue" =>
        Some(new PropertyDistinctValueReport(
          inputURI, outputURI, sparkMasterName, reportParams
        ))
      case "propertyValue" =>
        Some(new PropertyValueReport(
          inputURI, outputURI, sparkMasterName, reportParams
        ))
      case _ => None
    }
  }

  def main(): Unit = {
    val reportResult: Try[Unit] = getReport(token) match {
      case Some(report) => report.run()
      case None => Failure(
        new RuntimeException(s"Report type $token is unknown")
      )
    }
    reportResult match {
      case Failure(ex) =>
        logger.error(ex.toString)
        logger.error("\n" + ex.getStackTrace.mkString("\n"))
      case _ => Unit
    }
  }
}


object ReporterMain {

  def usage(): Unit = {
    println(
      """
        |Usage:
        |
        |ReporterMain <input> <output> <spark master> <report token> \
        |            [<param> ...]
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
    val reportParams = args.slice(4, args.length)
    new Reporter(
      token, inputURI, outputURI, sparkMasterName, reportParams
    ).main()
  }
}
