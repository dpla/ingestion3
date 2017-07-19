package dpla.ingestion3.enrichments

import dpla.ingestion3.model.EdmTimeSpan
import org.scalatest.{BeforeAndAfter, FlatSpec}

class ParseDateEnrichmentTest extends FlatSpec with BeforeAndAfter {
  val enrichment = new ParseDateEnrichment

  "ParseDateEnrichment" should "parse calendar to iso Date'" in {
    val date = "May 15, 2014"
    val originalDate = EdmTimeSpan(
      originalSourceDate = Some(date),
      prefLabel = None,
      begin = None,
      end = None
    )
    val enrichedDate = EdmTimeSpan(
      originalSourceDate = Some(date),
      prefLabel = Some("2014-05-15"),
      begin = None,
      end = None
    )

    assert(enrichment.parse(originalDate) === enrichedDate)
  }


  it should "parse iso to iso Date" in {
    val date = "2014-05-15"
    val originalDate = EdmTimeSpan(originalSourceDate = Some(date))
    val enrichedDate = EdmTimeSpan(
      originalSourceDate = Some(date),
      prefLabel = Some("2014-05-15"),
      begin = None,
      end = None
    )

    assert(enrichment.parse(originalDate) === enrichedDate)
  }


  it should "parse slash date to iso Date" in {
    val date = "5/7/2012"
    val originalDate = EdmTimeSpan(originalSourceDate = Some(date))
    val enrichedDate = EdmTimeSpan(
      originalSourceDate = Some(date),
      prefLabel = Some("2012-05-07"),
      begin = None,
      end = None
    )

    assert(enrichment.parse(originalDate) === enrichedDate)
  }

  it should "parse dot date to iso date" in {
    val date = "5.7.2012"
    val originalDate = EdmTimeSpan(originalSourceDate = Some(date))
    val enrichedDate = EdmTimeSpan(
      originalSourceDate = Some(date),
      prefLabel = Some("2012-05-07"),
      begin = None,
      end = None
    )

    assert(enrichment.parse(originalDate) === enrichedDate)
  }

  it should "parse Month, Year to iso date" in {
    val date = "July, 2015"
    val originalDate = EdmTimeSpan(originalSourceDate = Some(date))
    val enrichedDate = EdmTimeSpan(
      originalSourceDate = Some(date),
      prefLabel = Some("2015-07-01"),
      begin = None,
      end = None
    )

    assert(enrichment.parse(originalDate) === enrichedDate)
  }

  it should "parse M-D-Y to iso date" in {
    val date = "12-19-2010"
    val originalDate = EdmTimeSpan(originalSourceDate = Some(date))
    val enrichedDate = EdmTimeSpan(
      originalSourceDate = Some(date),
      prefLabel = Some("2010-12-19"),
      begin = None,
      end = None
    )

    assert(enrichment.parse(originalDate) === enrichedDate)
  }

//  ignore should "parses uncertain to EDTF" in //todo edtf
//    assert(enrichment.parse("2015?") === Some("2015?"))

  it should "returns None for unparsable dates" in {
    val date = "emefragramoofabits"
    val originalDate = EdmTimeSpan(originalSourceDate = Some(date))
    val enrichedDate = EdmTimeSpan(
      originalSourceDate = Some(date),
      prefLabel = None,
      begin = None,
      end = None
    )

    assert(enrichment.parse(originalDate) === enrichedDate)
  }
}
