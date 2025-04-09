package dpla.ingestion3.model

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec


class UriTest extends AnyFlatSpec with BeforeAndAfter {

  "isValidEdmRightsUri" should "return `true` when URI is in list of approved URIs" in {
    val uri = URI("http://rightsstatements.org/vocab/CNE/1.0/")
    assert(uri.isValidEdmRightsUri === true)
  }

  it should "return `false` when URI is in list of approved URIs" in {
    val uri = URI("http://rightsstatements.org/vocab/cne/1.0/") // case sensitive, should not match
    assert(uri.isValidEdmRightsUri === false)
  }
}
