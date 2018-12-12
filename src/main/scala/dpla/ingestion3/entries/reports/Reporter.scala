package dpla.ingestion3.entries.reports

import dpla.ingestion3.model.OreAggregation
import dpla.ingestion3.reports._
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.{Failure, Try}

/**
  * Reporter, the report driver class.
  *
  * This is a class that is instantiated by ReporterMain because the original
  * idea was it would be possible to unit test this way.  Note that getReport()
  * is probably the only method that is worth testing.  See the unit tests in
  * ReporterTest.
  *
  * @see ReporterTest
  * @param sparkConf        Spark configuration
  * @param inputURI         Input URI or file path
  * @param outputURI        Output URI or file path
  * @param token            The report name token
  * @param reportParams     Additional parameters particular to the report
  */
class Reporter (
                 spark: SparkSession,
                 token: String,
                 input: Dataset[OreAggregation],
                 reportParams: Array[String] = Array(),
                 logger: Logger
               ) {

  private def getReport(token: String): Option[Report] = {
    token match {
      case "propertyDistinctValue" =>
        Some(new PropertyDistinctValueReport(input, spark, reportParams))
      case "propertyValue" =>
        Some(new PropertyValueReport(input, spark, reportParams))
      case "metadataCompleteness" =>
        Some(new MetadataCompletenessReport(input, spark, reportParams))
      case "thumbnail" =>
        Some(new ThumbnailReport(input, spark, reportParams))
      case _ => None
    }
  }

  def main(): Try[DataFrame] = {
    getReport(token) match {
      case Some(report) => report.run()
      case None => Failure(
        new RuntimeException(s"Report type $token is unknown")
      )
    }
  }
}