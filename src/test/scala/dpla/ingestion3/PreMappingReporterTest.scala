package dpla.ingestion3

import org.scalatest.{FlatSpec, PrivateMethodTester}
import org.scalamock.scalatest.MockFactory
import dpla.ingestion3.premappingreports._

class PreMappingReporterTest extends FlatSpec
    with MockFactory with PrivateMethodTester {

  "PreMappingReporter.getPreMappingReport" should "return a XmlShredder given token " +
      "'xmlShredder'" in {
    val getPreMappingReport = PrivateMethod[Option[PreMappingReport]]('getPreMappingReport)
    val preMappingReporter = new PreMappingReporter("x", "x", "x", "x")
    val preMappingReport = preMappingReporter invokePrivate getPreMappingReport("xmlShredder")
    preMappingReport match {
      case Some(r) => assert(r.isInstanceOf[XmlShredderReport])
      case _ => fail
    }
  }
}
