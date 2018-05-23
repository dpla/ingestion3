package dpla.ingestion3.enrichments

import dpla.ingestion3.enrichments.date.DatePatternMatch
import org.scalacheck.{Gen, Prop}
import org.scalatest._
import org.scalacheck.Prop.forAll
import org.scalatest.prop.Checkers

class DatePatternMatchTest extends FlatSpec with BeforeAndAfter with Matchers with Checkers {
  val datePatternMatcher = new DatePatternMatch()

  val yyyyGen: Gen[Int] = Gen.choose(1000,2018)
  val delimGen: Gen[String] = Gen.oneOf("-", "/", " ")
  val monthGen: Gen[String] = Gen.oneOf("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul",
    "Aug", "Sep", "Oct", "Nov", "Dec")

  val yearMonthGen: Gen[String] = for {
    n <- yyyyGen
    m <- delimGen
    o <- monthGen
  } yield n + m + o

  "identifyPattern" should "match any year between 1000 and 2019" in {
    check(forAll(yyyyGen) { n => {
      datePatternMatcher.identifyPattern(n.toString).isDefined
    }})
  }

  it should "match yyyy MMM generated values" in {
    check(forAll(yearMonthGen) { n => {
      println(n)
      datePatternMatcher.identifyPattern(n) === Some("yyyy MMM")
    }})
  }
}
