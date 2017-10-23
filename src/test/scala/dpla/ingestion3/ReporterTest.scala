package dpla.ingestion3

import dpla.ingestion3.entries.reports.Reporter
import org.scalatest.{FlatSpec, PrivateMethodTester}
import org.scalamock.scalatest.MockFactory
import dpla.ingestion3.reports._
import dpla.ingestion3.utils.Utils
import org.apache.spark.SparkConf


class ReporterTest extends FlatSpec
    with MockFactory with PrivateMethodTester {

  "Reporter.getReport" should "return a PropertyDistinctValueReport given token " +
      "'propertyDistinctValue'" in {
    val getReport = PrivateMethod[Option[Report]]('getReport)
    val reporter = new Reporter(new SparkConf(), "x", "x", "x", Array(), Utils.createLogger("rpt"))
    val report = reporter invokePrivate getReport("propertyDistinctValue")
    report match {
      case Some(r) => assert(r.isInstanceOf[PropertyDistinctValueReport])
      case _ => fail
    }
  }

  it should "return a PropertyValueReport given token " +
    "'propertyValue'" in {
    val getReport = PrivateMethod[Option[Report]]('getReport)
    val reporter = new Reporter(new SparkConf(), "x", "x", "x", Array(), Utils.createLogger("rpt"))
    val report = reporter invokePrivate getReport("propertyValue")
    report match {
      case Some(r) => assert(r.isInstanceOf[PropertyValueReport])
      case _ => fail
    }
  }

  it should "return an empty result for an invalid token" in {
    val getReport = PrivateMethod[Option[Report]]('getReport)
    val reporter = new Reporter(new SparkConf(), "x", "x", "x", Array(), Utils.createLogger("rpt"))
    val r = reporter invokePrivate getReport("x")
    assert(r.isEmpty)
  }

}
