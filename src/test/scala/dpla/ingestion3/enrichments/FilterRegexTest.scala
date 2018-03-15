package dpla.ingestion3.enrichments

import dpla.ingestion3.enrichments.FilterRegex._
import org.scalatest.{BeforeAndAfter, FlatSpec}
import org.scalatest._

import scala.util.matching

class FilterRegexTest extends FlatSpec with BeforeAndAfter with Matchers {

  // Test terms
  val singleWordBlockTerm = "jpeg"
  val multiWordBlockTerm = "jpeg 2000"
  // Test regular expressions
  val singleWordBlockTermRegex: matching.Regex = singleWordBlockTerm.blockListRegex.r
  val multiWordBlockTermRegex: matching.Regex = multiWordBlockTerm.blockListRegex.r

  // TODO Which of these testing styles is preferred?

  // TODO Test: Overlap with single and multi-word block words (jpeg <> jpeg 2000)
  // TODO TEST: Ignore (some) punctuation
  // TODO TEST: Plurals
  // TODO TEST: Live data and complex examples
  // TODO TEST: All allowListRegex tests

  // Exact term match
  s"blockListRegex" should "match when the terms are the same" in {
    singleWordBlockTerm should fullyMatch regex singleWordBlockTermRegex
    multiWordBlockTerm should fullyMatch regex multiWordBlockTermRegex
  }

  // Case-insensitive
   it should "ignore case" in {
    "Jpeg" should fullyMatch regex singleWordBlockTermRegex
    "JPEG" should fullyMatch regex singleWordBlockTermRegex
     "JPEG 2000" should fullyMatch regex multiWordBlockTermRegex
     "jPEg 2000" should fullyMatch regex multiWordBlockTermRegex
  }

  it should "ignore leading and trailing white space(s)" in {
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
  it should "match all occurrences when a term is repeated" in {
    // Single-word term repeated
    assert((singleWordBlockTermRegex findAllMatchIn "jpeg jpeg" mkString "") === "jpeg jpeg")
    assert((singleWordBlockTermRegex findAllMatchIn "   jpeg   jpeg  " mkString "") === "   jpeg   jpeg  ")
    assert((singleWordBlockTermRegex findAllMatchIn "jpeg tiff jpeg" mkString "") === "jpeg  jpeg")
    // Multi-word term repeated
    assert((multiWordBlockTermRegex findAllMatchIn "jpeg 2000 jpeg 2000" mkString "") === "jpeg 2000 jpeg 2000")
    assert((multiWordBlockTermRegex findAllMatchIn "   jpeg 2000  jpeg 2000 " mkString "") === "   jpeg 2000  jpeg 2000 ")
    assert((multiWordBlockTermRegex findAllMatchIn "jpeg tiff jpeg 2000" mkString "") === " jpeg 2000")
  }

  s"blockListRegex with single-word term" should "match when terms are separated or surrounded by white space" in {
    // Single-word no leading or trailing, only separating
    assert((singleWordBlockTermRegex findFirstMatchIn "jpeg photo").get.matched === "jpeg ")
    assert((singleWordBlockTermRegex findFirstMatchIn "jpeg  photo").get.matched === "jpeg  ")

    // Single-word leading white space
    assert((singleWordBlockTermRegex findFirstMatchIn " jpeg photo").get.matched   === " jpeg ")
    assert((singleWordBlockTermRegex findFirstMatchIn " jpeg  photo").get.matched  === " jpeg  ")
    assert((singleWordBlockTermRegex findFirstMatchIn "  jpeg photo").get.matched  === "  jpeg ")
    assert((singleWordBlockTermRegex findFirstMatchIn "  jpeg  photo").get.matched === "  jpeg  ")

    // Single-word trailing white space
    assert((singleWordBlockTermRegex findFirstMatchIn "jpeg photo ").get.matched === "jpeg ")
    assert((singleWordBlockTermRegex findFirstMatchIn "jpeg photo  ").get.matched === "jpeg ")
    assert((singleWordBlockTermRegex findFirstMatchIn "jpeg  photo ").get.matched === "jpeg  ")
    assert((singleWordBlockTermRegex findFirstMatchIn "jpeg  photo  ").get.matched === "jpeg  ")

    // Single-word leading and trailing white space
    assert((singleWordBlockTermRegex findFirstMatchIn " jpeg photo ").get.matched === " jpeg ")
    assert((singleWordBlockTermRegex findFirstMatchIn " jpeg  photo ").get.matched === " jpeg  ")
    assert((singleWordBlockTermRegex findFirstMatchIn " jpeg photo  ").get.matched === " jpeg ")
    assert((singleWordBlockTermRegex findFirstMatchIn "  jpeg photo  ").get.matched === "  jpeg ")
    assert((singleWordBlockTermRegex findFirstMatchIn "  jpeg  photo ").get.matched === "  jpeg  ")
    assert((singleWordBlockTermRegex findFirstMatchIn "  jpeg photo  ").get.matched === "  jpeg ")
    assert((singleWordBlockTermRegex findFirstMatchIn "  jpeg  photo  ").get.matched === "  jpeg  ")
  }
}
