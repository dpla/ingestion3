package dpla.ingestion3.enrichments

import dpla.ingestion3.model.EdmTimeSpan
import org.scalatest.{BeforeAndAfter, FlatSpec, PrivateMethodTester}

class ParseDateEnrichmentTest extends FlatSpec
    with BeforeAndAfter
    with PrivateMethodTester {

  val enrichment = new ParseDateEnrichment
  val preprocess: PrivateMethod[String] = PrivateMethod[String]('preprocess)

  "ParseDateEnrichment.preprocess" should "remove 'late' and 'early'" in {
    // val preprocess = PrivateMethod[String]('preprocess)
    val strings = List("Late 1910s", "early 2000s", "circa Early 1920s")
    for (s <- strings) {
      val rv = enrichment invokePrivate preprocess(s)
      assert(! (rv matches """.*[Ll]ate.*"""))
      assert(! (rv matches """.*[Ee]arly.*"""))
    }
  }

  it should "normalize whitespace" in {
    val rv = enrichment invokePrivate preprocess("1900 to  1920")
    assert(rv == "1900 to 1920")
  }

  it should "simplify decades by using 'x' notation" in {
    val strings = List("1980s", "2010s", "early 560s")
    for (s <- strings) {
      val rv = enrichment invokePrivate preprocess(s)
      assert(rv matches """^\d{2,3}x$""")
    }
  }

  // See comment in preprocess(). Why do this?
  it should "replace unterminated ranges with 'x' notation" in {
    val strings = List("1978--", "1901-")  // to "1978xx" and "1901x"
    for (s <- strings) {
      val rv = enrichment invokePrivate preprocess(s)
      assert(rv matches """\d{2,4}x+$""")
    }
  }

  it should "not alter an exact EDTF date" in {
    val strings = List("2017-10-04", "2017-10", "2017")
    for (s <- strings) {
      val rv = enrichment invokePrivate preprocess(s)
      assert(rv == s)
    }
  }

  it should "not alter an exact EDTF date and time (timestamp)" in {
    val strings = List(
      "2001-02-03T09:30:01",
      "2004-01-01T10:10:10Z",
      "2004-01-01T10:10:10+05:00"
    )
    for (s <- strings) {
      val rv = enrichment invokePrivate preprocess(s)
      assert(rv == s)
    }
  }

  it should "not alter an EDTF interval" in {
    val strings = List(
      "1964/2008",
      "2004-06/2006-08",
      "2004-02-01/2005-02-08",
      "2004-02-01/2005-02",
      "2004-02-01/2005",
      "2005/2006-02"
    )
    for (s <- strings) {
      val rv = enrichment invokePrivate preprocess(s)
      assert(rv == s)
    }
  }

  "ParseDateEnrichment" should "parse calendar to iso Date'" in {
    val date = "Jan 10, 2011"
    val originalDate = EdmTimeSpan(originalSourceDate = Some(date))
    val enrichedDate = EdmTimeSpan(
      originalSourceDate = Some(date),
      prefLabel = Some("2011-01-10"),
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
      prefLabel = Some(date),
      begin = Some(date),
      end = Some(date)
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
    val date = "9.8.2013"
    val originalDate = EdmTimeSpan(originalSourceDate = Some(date))
    val enrichedDate = EdmTimeSpan(
      originalSourceDate = Some(date),
      prefLabel = Some("2013-09-08"),
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

  "ParseDateEnrichment.edtfExactDate" should "return timespan for EDTF date" in {
    val strings = List("2001-02-03", "2008-12", "2008")
    for (s <- strings) {
      val rv: Option[EdmTimeSpan] = enrichment.edtfExactDate(s)
      val ts = rv.get
      assert(ts.begin.getOrElse("") == s)
      assert(ts.end.getOrElse("") == s)
    }
  }

  "ParseDateEnrichment.edtfDateAndTime" should "return timespan for EDTF " +
      "Date and Time timestamp" in {
    val strings = List(
      "2001-02-03T09:30:01",
      "2004-01-01T10:10:10Z",
      "2004-01-01T10:10:10+05:00"
    )
    // Dates correspond to the dates in the timestamps above.
    val dates = List(
      "2001-02-03",
      "2004-01-01",
      "2004-01-01"
    )
    for (i <- 0 to 2) {
      val rv: Option[EdmTimeSpan] = enrichment.edtfDateAndTime(strings(i))
      val ts = rv.get
      assert(ts.begin.getOrElse("") == dates(i))
      assert(ts.end.getOrElse("") == dates(i))
    }
  }

  "ParseDateEnrichment.edtfInterval" should "return timespan for EDTF " +
      "Interval" in {
    val strings = List(
      "1964/2008",
      "2004-06/2006-08",
      "2004-02-01/2005-02-08",
      "2004-02-01/2005-02",
      "2004-02-01/2005",
      "2005/2006-02"
    )
    val dates = List(
      List("1964", "2008"),
      List("2004-06", "2006-08"),
      List("2004-02-01", "2005-02-08"),
      List("2004-02-01", "2005-02"),
      List("2004-02-01", "2005"),
      List("2005", "2006-02")
    )
    for (i <- 0 to 5) {
      val rv: Option[EdmTimeSpan] = enrichment.edtfInterval(strings(i))
      val ts = rv.get
      assert(ts.begin.getOrElse("") == dates(i)(0))
      assert(ts.end.getOrElse("") == dates(i)(1))
    }
  }

  "ParseDateEnrichment.timeSpanFromEDTF" should "return an EdmTimeSpan for a " +
      "valid EDTF string" in {
    val strings = List(
      "1964/2008",
      "2004-01-01T10:10:10Z",
      "2001-02-03"
    )
    val dates = List(
      List("1964", "2008"),
      List("2004-01-01", "2004-01-01"),
      List("2001-02-03", "2001-02-03")
    )
    for (i <- 0 to 2) {
      val rv: Option[EdmTimeSpan] = enrichment.timeSpanFromEDTF(strings(i))
      val ts = rv.get
      assert(ts.begin.getOrElse("") == dates(i)(0))
      assert(ts.end.getOrElse("") == dates(i)(1))
    }
  }

}
