package la.dp.ingestion3

import java.net.URL

import org.scalatest.{FlatSpec, Matchers}

class OaiQueryUrlBuilderTest extends FlatSpec with Matchers {
  val endpoint: URL = new URL("http://aggregator.padigital.org/oai")
  val metadataPrefix = "oai_dc"

  "buildQueryUrl (when given an empty resumptionToken)" should "return an java.net.URL object " +
    "with a metadataPrefix property." in {
//    val url = new OaiQueryUrlBuilder().buildQueryUrl(endpoint, metadataPrefix, "", "ListRecords")
//    assert(url.isInstanceOf[java.net.URL])
//    assert(url.getQuery.contains("metadataPrefix="+metadataPrefix))
//    assert(!url.getQuery.contains("resumptionToken="))
  }

  "buildQueryUrl (when given a resumptionToken) " should " return a URL with a resumptionToken property " +
    "and no metadataPrefix property" in {
//    val queryUrl = new OaiQueryUrlBuilder().buildQueryUrl(endpoint, metadataPrefix, "token1", "ListRecords")
//    assert(queryUrl.isInstanceOf[URL])
  }
}
