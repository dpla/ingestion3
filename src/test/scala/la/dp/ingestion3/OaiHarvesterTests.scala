package la.dp.ingestion3

import java.io.File

import la.dp.ingestion3.harvesters.OaiHarvester
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest._

/**
  * Created by scott on 1/21/17.
  */
class OaiHarvesterTests extends FlatSpec with Matchers {
  val file = new File("/dev/null")
  val oai_url = new java.net.URL("http://dev.com")
  val harvester = new OaiHarvester(oai_url, "ListRecords", file)

  it should " throw an HarvesterException if a badArgument error code is returned" in {
    val errorCode = "badArguement"
    assertThrows[Exception] {
      harvester.checkOaiErrorCode(errorCode)
    }
  }

  it should " return an empty String when getting resumptionToken if there is an error in the response " in {
    val xml = la.dp.ingestion3.data.TestOaiData.pa_error
    assert( harvester.getResumptionToken(xml) == "" )
  }

  it should " return the String value of the resumptionToken if the response is valid" in {
    val xml = la.dp.ingestion3.data.TestOaiData.pa_oai
    assert( harvester.getResumptionToken(xml) == "90d421891feba6922f57a59868d7bcd1" )
  }
}
