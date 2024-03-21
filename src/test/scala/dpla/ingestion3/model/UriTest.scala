package dpla.ingestion3.model

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec


class UriTest extends AnyFlatSpec with BeforeAndAfter {

//  "normalize URI" should "replace `/page/` with `/vocab/`" in {
//    val uri = URI("http://rightsstatements.org/page/CNE/1.0/")
//    assert(uri.normalize === "http://rightsstatements.org/vocab/CNE/1.0/")
//  }
//
//  it should "replace https:// with http://" in {
//    val uri = URI("https://rightsstatements.org/vocab/CNE/1.0/")
//    assert(uri.normalize === "http://rightsstatements.org/vocab/CNE/1.0/")
//  }
//
//  it should "remove double forward slashes" in {
//    val uri = URI("http://rightsstatements.org/vocab/CNE/1.0//")
//    assert(uri.normalize === "http://rightsstatements.org/vocab/CNE/1.0/")
//  }
//
//  it should "add a single forward slash at the end" in {
//    val uri = URI("http://rightsstatements.org/vocab/CNE/1.0")
//    assert(uri.normalize === "http://rightsstatements.org/vocab/CNE/1.0/")
//  }
//
//  it should "return the original value if not given a URI" in {
//    val uri = URI("c:\\media\\image.jpg")
//    assert(uri.normalize === "c:\\media\\image.jpg")
//  }

//  it should "strip a trailing ; from a URI" in {
//    val uri = URI("http://rightsstatements.org/vocab/CNE/1.0;")
//    assert(uri.normalize === "http://rightsstatements.org/vocab/CNE/1.0/")
//  }

  "isValidEdmRightsUri" should "return `true` when URI is in list of approved URIs" in {
    val uri = URI("http://rightsstatements.org/vocab/CNE/1.0/")
    assert(uri.isValidEdmRightsUri === true)
  }

  it should "return `false` when URI is in list of approved URIs" in {
    val uri = URI("http://rightsstatements.org/vocab/cne/1.0/") // case sensitive, should not match
    assert(uri.isValidEdmRightsUri === false)
  }
}
