package dpla.ingestion3.utils

import dpla.ingestion3.utils.EDTFDate.DateRangeStrings
import org.scalatest.FlatSpec


class EDTFDateTest extends FlatSpec {

  case class tuple2(a: String, b: String)
  case class tuple3(a: String, b: String, c: String)

  "EDTFDate.rangeForExactDate" should "return timespan for EDTF date" in {
    val dates = List(
      tuple2("1", "1"),
      tuple2("2001-02-03", "2001-02-03"),
      tuple2("2008-12", "2008-12"),
      tuple2("2008", "2008"),
      tuple2("-2008", "-2008"),
      tuple2("y19840", "19840"),
      tuple2("y-19840", "-19840"),
      tuple2("1984?", "1984"),
      tuple2("2004-06?", "2004-06"),
      tuple2("2004-06-11?", "2004-06-11"),
      tuple2("1984~", "1984"),
      tuple2("1984?~", "1984"),
      tuple2("2017-21", "2017") // season syntax, e.g. Spring, 2017.
    )
    for (d <- dates) {
      val rv = EDTFDate.rangeForExactDate(d.a)
      assert(rv.getOrElse(("", "")) == DateRangeStrings(d.b, d.b))
    }
  }

  it should "return a range for certain unspecified dates" in {
    val dates = List(
      tuple3("199u", "1990", "1999"),
      tuple3("19uu", "1900", "1999"),
      // Level 1 spec says day precision for "1999-01-uu but we're leaving that
      // for later -- too much work at the moment to find days per month
      tuple3("1999-01-uu", "1999-01", "1999-01"),
      tuple3("1999-uu-uu", "1999-01-01", "1999-12-31")
    )
    for (d <- dates) {
      val rv = EDTFDate.rangeForExactDate(d.a)
      assert(rv.getOrElse(("", "")) == DateRangeStrings(d.b, d.c))
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
      val rv = EDTFDate.rangeForExactDate(d)
      assert(rv.getOrElse(None) == None)
    }
  }

  "EDTFDate.rangeForDateAndTime" should "return timespan for EDTF Date and " +
      "Time timesatmp" in {
    val dates = List(
      tuple2("2001-02-03T09:30:01", "2001-02-03"),
      tuple2("2004-01-01T10:10:10Z", "2004-01-01"),
      tuple2("2004-01-01T10:10:10+05:00", "2004-01-01")
    )
    for (d <- dates) {
      val rv = EDTFDate.rangeForDateAndTime(d.a)
      assert(rv.getOrElse(("", "")) == DateRangeStrings(d.b, d.b))
    }
  }

  "EDTFDate.rangeForInterval" should "return timespan for EDTF Interval" in {
    val dates = List(
      tuple3("1964/2008", "1964", "2008"),
      tuple3("2004-06/2006-08", "2004-06", "2006-08"),
      tuple3("2004-02-01/2005-02-08", "2004-02-01", "2005-02-08"),
      tuple3("2004-02-01/2005-02", "2004-02-01", "2005-02"),
      tuple3("2004-02-01/2005", "2004-02-01", "2005"),
      tuple3("2005/2006-02", "2005", "2006-02"),
      tuple3("1984~/2004-06", "1984", "2004-06"),
      tuple3("1984/2004-06~", "1984", "2004-06"),
      tuple3("1984?/2004?~", "1984", "2004"),
      tuple3("y-10000/2000", "-10000", "2000"),
      tuple3("2017/y10000", "2017", "10000"),
      tuple3("unknown/1984", "", "1984"),
      tuple3("644/unknown", "644", "")
    )
    for (d <- dates) {
      val rv = EDTFDate.rangeForInterval(d.a)
      assert(rv.getOrElse(("", "")) == DateRangeStrings(d.b, d.c))
    }
  }

  it should "not provide dates for invalid strings" in {
    val dates = List(
      "-unknown/1984",           // negative not allowed
      "unknown-10-10/2017-1-1"   // "unknown" must be whole date
    )
    for (d <- dates) {
      val rv = EDTFDate.rangeForInterval(d)
      assert(rv.getOrElse(None) == None)
    }
  }

  "EDTFDate.rangeForOpenInterval" should "return timespan for EDTF 'open' " +
      "interval" in {
    val dates = List(
      tuple3("1900/open", "1900", ""),
      tuple3("y21000/open", "21000", ""),
      tuple3("y-10000/open", "-10000", "")
    )
    for (d <- dates) {
      val rv = EDTFDate.rangeForOpenInterval(d.a)
      assert(rv.getOrElse(("", "")) == DateRangeStrings(d.b, d.c))
    }
  }

  it should "not provide dates for invalid strings" in {
    val dates = List(
      "unknown/open",
      "open/open"
    )
    for (d <- dates) {
      val rv = EDTFDate.rangeForOpenInterval(d)
      assert(rv.getOrElse(None) == None)
    }
  }

  "EDTFDate.rangeForEDTF" should "handle the gamut of different patterns" in {
    // Representative sample of each thing above
    val dates = List(
      tuple3("2001-02-03", "2001-02-03", "2001-02-03"),
      tuple3("199u", "1990", "1999"),
      tuple3("2001-02-03T09:30:01", "2001-02-03", "2001-02-03"),
      tuple3("1964/2008", "1964", "2008"),
      tuple3("2017/y10000", "2017", "10000"),
      tuple3("1984~/2004-06", "1984", "2004-06"),
      tuple3("644/unknown", "644", ""),
      tuple3("1900/open", "1900", "")
    )
    for (d <- dates) {
      val rv = EDTFDate.rangeForEDTF(d.a)
      assert(rv.getOrElse("", "") == DateRangeStrings(d.b, d.c))
    }
  }
}
