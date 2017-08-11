package dpla.ingestion3.reports

import org.scalatest.FlatSpec
import org.scalamock.scalatest.MockFactory
import org.apache.spark.sql.{DataFrame, Row, SparkSession, Dataset}
import dpla.ingestion3.model._
import dpla.ingestion3.reports._


class PropertyDistinctValueReportTest extends FlatSpec with MockFactory{

  "PropertyDistinctValueReport.process" should "x" in {
    // FIXME: How do you mock Dataframes and DataSets??
    true
  }

}
