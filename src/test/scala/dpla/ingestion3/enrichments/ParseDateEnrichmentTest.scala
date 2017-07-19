package dpla.ingestion3.enrichments

import org.scalatest.{BeforeAndAfter, FlatSpec}

class ParseDateEnrichmentTest extends FlatSpec with BeforeAndAfter {
  val enrichment = new ParseDateEnrichment

  "ParseDateEnrichment" should "parse calendar to iso Date'" in
    assert(enrichment.parse("May 15, 2014") === Some("2014-05-15"))

  it should "parse iso to iso Date" in
    assert(enrichment.parse("2014-05-15") === Some("2014-05-15"))

  it should "parse slash date to iso Date" in
    assert(enrichment.parse("5/7/2012") === Some("2012-05-07"))

  it should "parse dot date to iso date" in
    assert(enrichment.parse("5.7.2012") === Some("2012-05-07"))

  it should "parse Month, Year to iso date" in
    assert(enrichment.parse("July, 2015") === Some("2015-07-01"))

  it should "parse M-D-Y to iso date" in
    assert(enrichment.parse("12-19-2010") === Some("2010-12-19"))

  ignore should "parses uncertain to EDTF" in //todo edtf
    assert(enrichment.parse("2015?") === Some("2015?"))

  it should "returns None for unparsable dates" in
    assert(enrichment.parse("emefragramoofabits") === None)
}
