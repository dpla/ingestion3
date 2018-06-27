package dpla.ingestion3.enrichments

import dpla.ingestion3.enrichments.date.DatePatternMatch
import org.scalacheck.{Gen, Prop}
import org.scalatest._
import org.scalacheck.Prop.forAll
import org.scalatest.prop.Checkers

class DatePatternMatchTest extends FlatSpec with BeforeAndAfter with Matchers with Checkers {
  val datePatternMatcher = new DatePatternMatch()

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
    "28", "29", "30", "31")


  val delimGen: Gen[String] = Gen.oneOf("-", "/", " ")
  val monthStrGen: Gen[String] = Gen.oneOf(
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
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


  "identifyPattern" should "match any year between 1000 and 2019" in {
    check(forAll(yearGen) { n => {
      datePatternMatcher.identifyPattern(n.toString).isDefined
    }})
  }
  it should "match yyyy MMM generated values" in {

    val yearMonthGen: Gen[String] = for {
      y <- yearGen
      d1 <- delimGen
      m <- monthStrGen
    } yield y + d1 + m

    check( forAll(yearMonthGen) { datePatternMatcher.identifyPattern(_) === Some("yyyy MMM") })
  }

  it should "match MMM yyyy generated values" in {
    val monthYearGen: Gen[String] = for {
      m <- monthStrGen
      d1 <- delimGen
      y <- yearGen
    } yield m + d1 + y


    check( Prop.forAllNoShrink(monthYearGen) { p => {
      datePatternMatcher.identifyPattern(p) === Some("MMM yyyy")
    }})
  }

  it should "match yyyy MMM d(d) generated values" in {
    val yearMonthDayGen: Gen[String] = for {
      y <- yearGen
      d1 <- delimGen
      m <- monthStrGen
      d2 <- delimGen
      d <- dayGen
    } yield y + d1 + m + d2 + d

    check( forAll(yearMonthDayGen) { datePatternMatcher.identifyPattern(_) === Some("yyyy MMM d(d)") } )
  }

  it should "match yyyy MM dd generated values" in {
    val yearMonthDayGen: Gen[String] = for {
      y <- yearGen
      d1 <- delimGen
      m <- monthGen
      d2 <- delimGen
      d <- dayGen
    } yield y + d1 + m + d2 + d

    check( Prop.forAllNoShrink(yearMonthDayGen) { p =>
      println(p)
      datePatternMatcher.identifyPattern(p) === Some("yyyy m(m) d(d)") } )
  }

  it should "match yyyy m(m) generated values" in {
    val yearMonthGen: Gen[String] = for {
      y <- yearGen
      d1 <- delimGen
      m <- monthGen
    } yield y + d1 + m

    check( Prop.forAllNoShrink(yearMonthGen) { p => {
      println(p + " " + datePatternMatcher.identifyPattern(p))
      datePatternMatcher.identifyPattern(p) === Some("yyyy m(m)") } } )
  }

}
