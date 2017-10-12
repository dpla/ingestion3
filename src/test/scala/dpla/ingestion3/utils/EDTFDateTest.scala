package dpla.ingestion3.utils

import org.scalatest.FlatSpec


class EDTFDateTest extends FlatSpec {

  "EDTFDate.rangeForExactDate" should "return timespan for EDTF date" in {
    val dates = List(
      ("1", "1"),
      ("2001-02-03", "2001-02-03"),
      ("2008-12", "2008-12"),
      ("2008", "2008"),
      ("-2008", "-2008"),
      ("y19840", "19840"),
      ("y-19840", "-19840"),
      ("1984?", "1984"),
      ("2004-06?", "2004-06"),
      ("2004-06-11?", "2004-06-11"),
      ("1984~", "1984"),
      ("1984?~", "1984"),
      ("2017-21", "2017") // season syntax, e.g. Spring, 2017.
    )
    for (d <- dates) {
      val rv: Option[(String, String)] = EDTFDate.rangeForExactDate(d._1)
      assert(rv.getOrElse(("", "")) == (d._2, d._2))
    }
  }

  it should "return a range for certain unspecified dates" in {
    val dates = List(
      ("199u", "1990", "1999"),
      ("19uu", "1900", "1999"),
      // Level 1 spec says day precision for "1999-01-uu but we're leaving that
      // for later -- too much work at the moment to find days per month
      ("1999-01-uu", "1999-01", "1999-01"),
      ("1999-uu-uu", "1999-01-01", "1999-12-31")
    )
    for (d <- dates) {
      val rv: Option[(String, String)] = EDTFDate.rangeForExactDate(d._1)
      assert(rv.getOrElse(("", "")) == (d._2, d._3))
    }
  }

  it should "not return a timespan for some obviously invalid dates" in {
    // Also verifies the month and day handling of the other methods in
    // EDTFDate which use EDTFDate.monthPat and EDTFDate.dayPat.
    val dates = List(
      "2014-13-01",
      "1999-01-32",
      "2000-10-60",
      "2017-01-00",
      "2017-00-00"
    )
    // Known issue: it doesn't reject impossible dates like 2017-02-29,
    // 2017-11-31, etc.
    for (d <- dates) {
      val rv: Option[(String, String)] = EDTFDate.rangeForExactDate(d)
      assert(rv.getOrElse(None) == None)
    }
  }

  "EDTFDate.rangeForDateAndTime" should "return timespan for EDTF Date and " +
      "Time timesatmp" in {
    val dates = List(
      ("2001-02-03T09:30:01", "2001-02-03"),
      ("2004-01-01T10:10:10Z", "2004-01-01"),
      ("2004-01-01T10:10:10+05:00", "2004-01-01")
    )
    for (d <- dates) {
      val rv: Option[(String, String)] = EDTFDate.rangeForDateAndTime(d._1)
      assert(rv.getOrElse(("", "")) == (d._2, d._2))
    }
  }

  "EDTFDate.rangeForInterval" should "return timespan for EDTF Interval" in {
    val dates = List(
      ("1964/2008", "1964", "2008"),
      ("2004-06/2006-08", "2004-06", "2006-08"),
      ("2004-02-01/2005-02-08", "2004-02-01", "2005-02-08"),
      ("2004-02-01/2005-02", "2004-02-01", "2005-02"),
      ("2004-02-01/2005", "2004-02-01", "2005"),
      ("2005/2006-02", "2005", "2006-02"),
      ("1984~/2004-06", "1984", "2004-06"),
      ("1984/2004-06~", "1984", "2004-06"),
      ("1984?/2004?~", "1984", "2004"),
      ("y-10000/2000", "-10000", "2000"),
      ("2017/y10000", "2017", "10000"),
      ("unknown/1984", "", "1984"),
      ("644/unknown", "644", "")
    )
    for (d <- dates) {
      val rv: Option[(String, String)] = EDTFDate.rangeForInterval(d._1)
      assert(rv.getOrElse(("", "")) == (d._2, d._3))
    }
  }

  it should "not provide dates for invalid strings" in {
    val dates = List(
      "-unknown/1984",           // negative not allowed
      "unknown-10-10/2017-1-1"   // "unknown" must be whole date
    )
    for (d <- dates) {
      val rv: Option[(String, String)] = EDTFDate.rangeForInterval(d)
      assert(rv.getOrElse(None) == None)
    }
  }

  "EDTFDate.rangeForOpenInterval" should "return timespan for EDTF 'open' " +
      "interval" in {
    val dates = List(
      ("1900/open", "1900", ""),
      ("y21000/open", "21000", ""),
      ("y-10000/open", "-10000", "")
    )
    for (d <- dates) {
      val rv: Option[(String, String)] = EDTFDate.rangeForOpenInterval(d._1)
      assert(rv.getOrElse(("", "")) == (d._2, d._3))
    }
  }

  it should "not provide dates for invalid strings" in {
    val dates = List(
      "unknown/open",
      "open/open"
    )
    for (d <- dates) {
      val rv: Option[(String, String)] = EDTFDate.rangeForOpenInterval(d)
      assert(rv.getOrElse(None) == None)
    }
  }

  "EDTFDate.rangeForEDTF" should "handle the gamut of different patterns" in {
    // Representative sample of each thing above
    val dates = List(
      ("2001-02-03", "2001-02-03", "2001-02-03"),
      ("199u", "1990", "1999"),
      ("2001-02-03T09:30:01", "2001-02-03", "2001-02-03"),
      ("1964/2008", "1964", "2008"),
      ("2017/y10000", "2017", "10000"),
      ("1984~/2004-06", "1984", "2004-06"),
      ("644/unknown", "644", ""),
      ("1900/open", "1900", "")
    )
    for (d <- dates) {
      val rv: Option[(String, String)] = EDTFDate.rangeForEDTF(d._1)
      assert(rv.getOrElse("", "") == (d._2, d._3))
    }
  }
}
