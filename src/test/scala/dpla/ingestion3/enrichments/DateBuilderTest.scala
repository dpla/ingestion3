package dpla.ingestion3.enrichments

import dpla.ingestion3.enrichments.date.DateBuilder
import dpla.ingestion3.model.EdmTimeSpan
import org.apache.commons.lang.StringUtils
import org.scalacheck.{Gen, Prop}
import org.scalatest._
import org.scalatest.prop.Checkers

class DateBuilderTest extends FlatSpec with BeforeAndAfter with Matchers with Checkers {

  case class TestDate(
                          day: String = "",
                          month: String = "",
                          year: String = "",
                          delimiter: String = " "
                        )

  val dateBulder= new DateBuilder()

  val dayGen: Gen[String] = Gen.oneOf(
    "01", "1",
    "02", "2",
    "03", "3",
    "04", "4",
    "05", "5",
    "06", "6",
    "07", "7",
    "08", "8",
    "09", "9",
    "10", "11", "12",
    "13", "14", "15",
    "16", "17", "18",
    "19", "20", "21",
    "22", "23", "24",
    "25", "26", "27",
    "28")
  // "29", "30") // FIXME how to deal with Feb 29, Nov 31
  val delimGen: Gen[String] = Gen.oneOf(" ", "-", "/")
  val monthGen: Gen[String] = Gen.oneOf(
    "01", "1",
    "02", "2",
    "03", "3",
    "04", "4",
    "05", "5",
    "06", "6",
    "07", "7",
    "08", "8",
    "09", "9",
    "10", "11", "12")
  val monthStrGen: Gen[String] = Gen.oneOf(
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
  val yearGen: Gen[Int] = Gen.choose(1000,2018)

  val monthStrIntMap: Map[String, String] = Map(
    "Jan" -> "01",
    "Feb" -> "02",
    "Mar" -> "03",
    "Apr" -> "04",
    "May" -> "05",
    "Jun" -> "06",
    "Jul" -> "07",
    "Aug" -> "08",
    "Sep" -> "09",
    "Oct" -> "10",
    "Nov" -> "11",
    "Dec" -> "12"
  )

  /**
    * Generate a DateDateObj
    * @param monthGen
    * @return
    */
  def dateGenerator(monthGen: Gen[String] = monthGen): Gen[TestDate] = {
    for {
      day <- dayGen
      month <- monthGen
      year <- yearGen
      delim <- delimGen
    } yield TestDate(day, month, year.toString, delim)
  }

  it should "create valid EdmTimeSpan with begin and end dates for yyyy" in {
    check(Prop.forAllNoShrink(dateGenerator()) { date =>
      val dateStr = s"${date.year}"

      val expectedDate = EdmTimeSpan(
        originalSourceDate = Some(dateStr),
        prefLabel = Some(s"${date.year}"),
        begin = Some(s"${date.year}"),
        end = Some(s"${date.year}")
      )

      dateBulder.buildEdmTimeSpan(dateStr)  === expectedDate
    })
  }

  it should "create valid EdmTimeSpan with begin and end dates for yyyy MM" in {
    check(Prop.forAllNoShrink(dateGenerator()) { date =>
      val dateStr = s"${date.year}${date.delimiter}${date.month}"
      val monthPad = StringUtils.leftPad(date.month, 2, "0")

      val expectedDate = EdmTimeSpan(
        originalSourceDate = Some(dateStr),
        prefLabel = Some(s"${date.year}-$monthPad"),
        begin = Some(s"${date.year}-$monthPad"),
        end = Some(s"${date.year}-$monthPad")
      )

      dateBulder.buildEdmTimeSpan(dateStr)  === expectedDate
    })
  }

  it should "create valid EdmTimeSpan with begin and end dates for yyyy MM dd" in {
    check(Prop.forAllNoShrink(dateGenerator()) { date =>
      val dateStr = s"${date.year}${date.delimiter}${date.month}${date.delimiter}${date.day}"
      val monthPad = StringUtils.leftPad(date.month, 2, "0")
      val dayPad = StringUtils.leftPad(date.day, 2, "0")

      val expectedDate = EdmTimeSpan(
        originalSourceDate = Some(dateStr),
        prefLabel = Some(s"${date.year}-$monthPad-$dayPad"),
        begin = Some(s"${date.year}-$monthPad-$dayPad"),
        end = Some(s"${date.year}-$monthPad-$dayPad")
      )

      dateBulder.buildEdmTimeSpan(dateStr)  === expectedDate
    })
  }

  it should "create valid EdmTimeSpan with begin and end dates for yyyy MMM" in {
    check(Prop.forAllNoShrink(dateGenerator(monthGen = monthStrGen)) { date =>
      val dateStr = s"${date.year}${date.delimiter}${date.month}"
      val monthInt = monthStrIntMap.getOrElse(date.month, "00")

      val expectedDate = EdmTimeSpan(
        originalSourceDate = Some(dateStr),
        prefLabel = Some(s"${date.year}-$monthInt"),
        begin = Some(s"${date.year}-$monthInt"),
        end = Some(s"${date.year}-$monthInt")
      )

      dateBulder.buildEdmTimeSpan(dateStr)  === expectedDate
    })
  }

  it should "create valid EdmTimeSpan with begin and end dates for MMM yyyy" in {
    check(Prop.forAllNoShrink(dateGenerator(monthGen = monthStrGen)) { date =>
      val dateStr = s"${date.year}${date.delimiter}${date.month}"
      val monthInt = monthStrIntMap.getOrElse(date.month, "00")

      val expectedDate = EdmTimeSpan(
        originalSourceDate = Some(dateStr),
        prefLabel = Some(s"${date.year}-$monthInt"),
        begin = Some(s"${date.year}-$monthInt"),
        end = Some(s"${date.year}-$monthInt")
      )

      dateBulder.buildEdmTimeSpan(dateStr)  === expectedDate
    })
  }

  it should "create valid EdmTimeSpan with begin and end dates for yyyy MMM dd" in {
    check(Prop.forAllNoShrink(dateGenerator(monthGen = monthStrGen)) { date =>
      val dayPad = StringUtils.leftPad(date.day, 2, "0")
      val dateStr = s"${date.year}${date.delimiter}${date.month}${date.delimiter}${date.day}"
      val monthInt = monthStrIntMap.getOrElse(date.month, "00")

      val expectedDate = EdmTimeSpan(
        originalSourceDate = Some(dateStr),
        prefLabel = Some(s"${date.year}-$monthInt-$dayPad"),
        begin = Some(s"${date.year}-$monthInt-$dayPad"),
        end = Some(s"${date.year}-$monthInt-$dayPad")
      )

      dateBulder.buildEdmTimeSpan(dateStr)  === expectedDate
    })
  }

  it should "create a valid EdmTimeSpan with begin and end dates for yyyy mm dd" in {
    check(Prop.forAllNoShrink(dateGenerator()) { date =>
      val monthPad = StringUtils.leftPad(date.month, 2, "0")
      val dayPad = StringUtils.leftPad(date.day, 2, "0")
      val dateStr = s"${date.year}${date.delimiter}${date.month}${date.delimiter}${date.day}"

      val expectedDate = EdmTimeSpan(
        originalSourceDate = Some(dateStr),
        prefLabel = Some(s"${date.year}-$monthPad-$dayPad"),
        begin = Some(s"${date.year}-$monthPad-$dayPad"),
        end = Some(s"${date.year}-$monthPad-$dayPad")
      )

      dateBulder.buildEdmTimeSpan(dateStr)  === expectedDate
    })
  }
}
