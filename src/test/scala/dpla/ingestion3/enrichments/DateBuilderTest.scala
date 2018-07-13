package dpla.ingestion3.enrichments

import dpla.ingestion3.enrichments.date.DateBuilder
import org.apache.commons.lang.StringUtils
import org.joda.time.DateTime
import org.scalacheck.{Gen, Prop}
import org.scalatest._
import org.scalatest.prop.Checkers

class DateBuilderTest extends FlatSpec with BeforeAndAfter with Matchers with Checkers {

  case class DateTestObj(
                          day: String = "",
                          month: String = "",
                          year: String = "",
                          delimiter: String = " "
                        )

  val dateBulder= new DateBuilder()

  val yearGen: Gen[Int] = Gen.choose(1000,2018)

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

  val monthStrGen: Gen[String] = Gen.oneOf(
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

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

  val delim = "\\s*[\\/-]*\\s*"

  "buildDateObject" should "create a valid DateTime object for dates between 1000 and 2019" in {
    check(Prop.forAllNoShrink(yearGen) { date =>
      val dateStr = date.toString
      val expectedDate = Option(dateStr + "-01-01") // TODO should this return yyyy or yyyy-mm-dd
    val generatedDate = dateBulder.buildDateObject(dateStr) match {
      case Some(d) => Some(d.toString)
      case None => None
    }
      generatedDate === expectedDate
    })
  }

  it should "create a valid label for yyyy-mm-dd" in {
    val myGen = for {
      day <- dayGen
      month <- monthGen
      year <- yearGen
    } yield DateTestObj(day, month, year.toString)

    check(Prop.forAllNoShrink(myGen) { date =>
      val monthPad = StringUtils.leftPad(date.month, 2, "0")
      val dayPad = StringUtils.leftPad(date.day, 2, "0")
      val dateStr = s"${date.year}${date.delimiter}${date.month}${date.delimiter}${date.day}"
      val expectedDate = Option(s"${date.year}-$monthPad-$dayPad")
      val generatedDate = dateBulder.buildDateObject(dateStr) match {
        case Some(d) => Some(d.toString)
        case None => None
      }
      generatedDate === expectedDate
    })
  }

  it should "create a valid label for yyyy MMM" in {
    // val date = "1945 Mar"
    val myGen = for {
      month <- monthStrGen
      year <- yearGen
    } yield DateTestObj(month = month, year = year.toString)

    check(Prop.forAllNoShrink(myGen) { date =>
      val dateStr = s"${date.year}${date.delimiter}${date.month}"
      val expectedDate = Option(s"${date.year}-${monthStrIntMap.getOrElse(date.month, "00")}-01")
      val generatedDate = dateBulder.buildDateObject(dateStr) match {
        case Some(d) => Some(d.toString)
        case None => None
      }

      generatedDate === expectedDate
    })
  }

  it should "create a valid label for MMM yyyy" in {
    val date = "Nov 1842"
    val expectedDate = Option("1842-11-01")
    val generatedDate = dateBulder.buildDateObject(date) match {
      case Some(d) => Some(d.toString)
      case None => None
    }
    generatedDate === expectedDate
  }

  it should "create a valid label for yyyy MMM dd" in {
    val date = "1984 Nov 1"
    val expectedDate = Option("1984-11-01")
    val generatedDate = dateBulder.buildDateObject(date) match {
      case Some(d) => Some(d.toString)
      case None => None
    }
    generatedDate === expectedDate
  }

  it should "create a valid label for yyyy-MM" in {
    val date = "1984 11"
    val expectedDate = Option("1984-11-01")
    val generatedDate = dateBulder.buildDateObject(date) match {
      case Some(d) => Some(d.toString)
      case None => None
    }
    generatedDate === expectedDate
  }
}
