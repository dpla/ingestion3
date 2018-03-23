package dpla.ingestion3.enrichments

import dpla.ingestion3.enrichments.normalizations.FilterRegex._
import org.scalatest._

import scala.util.matching

class FilterRegexTest extends FlatSpec with BeforeAndAfter with Matchers {

  // Test terms
  val singleWordBlockTerm = "jpeg"
  val multiWordBlockTerm = "jpeg 2000"
  // Test regular expressions
  val singleWordBlockTermRegex: matching.Regex = singleWordBlockTerm.blockListRegex.r
  val multiWordBlockTermRegex: matching.Regex = multiWordBlockTerm.blockListRegex.r


  // blockListRegex Tests
  s"blockListRegex" should "match when the terms are the same" in {
    singleWordBlockTerm should fullyMatch regex singleWordBlockTermRegex
    multiWordBlockTerm should fullyMatch regex multiWordBlockTermRegex
  }
   it should "ignore case" in {
    "Jpeg" should fullyMatch regex singleWordBlockTermRegex
    "JPEG" should fullyMatch regex singleWordBlockTermRegex
    "JPEG 2000" should fullyMatch regex multiWordBlockTermRegex
    "jPEg 2000" should fullyMatch regex multiWordBlockTermRegex
  }
  it should "consume leading and trailing white space(s)" in {
    // Leading space
    " jpeg" should fullyMatch regex singleWordBlockTermRegex // 1x
    "  jpeg" should fullyMatch regex singleWordBlockTermRegex // 2x
    " jpeg 2000" should fullyMatch regex multiWordBlockTermRegex // 1x
    "  jpeg 2000" should fullyMatch regex multiWordBlockTermRegex // 2x
    // Trailing space
    "jpeg " should fullyMatch regex singleWordBlockTermRegex // 1x
    "jpeg  " should fullyMatch regex singleWordBlockTermRegex // 2x
    "jpeg 2000 " should fullyMatch regex multiWordBlockTermRegex // 1x
    "jpeg 2000  " should fullyMatch regex multiWordBlockTermRegex // 2x
    // Leading and trailing single space
    " jpeg " should fullyMatch regex singleWordBlockTermRegex // 1x 1x
    " jpeg  " should fullyMatch regex singleWordBlockTermRegex // 1x 2x
    "  jpeg " should fullyMatch regex singleWordBlockTermRegex // 2x 1x
    "  jpeg   " should fullyMatch regex singleWordBlockTermRegex // 2x 2x
    " jpeg 2000 " should fullyMatch regex multiWordBlockTermRegex // 1x 1x
    " jpeg 2000  " should fullyMatch regex multiWordBlockTermRegex // 1x 2x
    "  jpeg 2000 " should fullyMatch regex multiWordBlockTermRegex // 2x 1x
    "  jpeg 2000   " should fullyMatch regex multiWordBlockTermRegex // 2x 2x
  }

  // AllowListRegex Tests
}
