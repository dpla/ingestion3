package dpla.ingestion3.enrichments

import dpla.ingestion3.enrichments.date.DateBuilder
import dpla.ingestion3.model.EdmTimeSpan
import org.scalatest._
import org.scalatest.prop.Checkers

class DateBuilderTest extends FlatSpec with BeforeAndAfter with Matchers with Checkers {

  val dateBuilder = new DateBuilder()

  it should "create a valid EdmTimeSpan with begin and end dates for yyyy-mm-dd" in {
    val dateStr = "2015-05-01"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some(dateStr),
      begin = Some(dateStr),
      end = Some(dateStr)
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }
  it should "create a valid EdmTimeSpan with begin and end dates for yyyy/mm/dd" in {
    val dateStr = "2015/05/01"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-05-01"),
      begin = Some("2015-05-01"),
      end = Some("2015-05-01")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }
  it should "create a valid EdmTimeSpan with begin and end dates for yyyy mm dd" in {
    val dateStr = "2015 05 01"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-05-01"),
      begin = Some("2015-05-01"),
      end = Some("2015-05-01")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }

  // yyyy-MMM-dd
  it should "create a valid EdmTimeSpan with begin and end dates for yyyy-MMM-dd" in {
    val dateStr = "2015-May-01"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-05-01"),
      begin = Some("2015-05-01"),
      end = Some("2015-05-01")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }
  it should "create a valid EdmTimeSpan with begin and end dates for yyyy/MMM/dd" in {
    val dateStr = "2015/May/01"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-05-01"),
      begin = Some("2015-05-01"),
      end = Some("2015-05-01")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }
  it should "create a valid EdmTimeSpan with begin and end dates for yyyy MMM dd" in {
    val dateStr = "2015 May 01"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-05-01"),
      begin = Some("2015-05-01"),
      end = Some("2015-05-01")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }

  // MMM-yyyy
  it should "create a valid EdmTimeSpan with begin and end dates for MMM-yyyy" in {
    val dateStr = "May-2015"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-05"),
      begin = Some("2015-05"),
      end = Some("2015-05")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }

  it should "create a valid EdmTimeSpan with begin and end dates for MMM/yyyy" in {
    val dateStr = "May/2015"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-05"),
      begin = Some("2015-05"),
      end = Some("2015-05")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }

  it should "create a valid EdmTimeSpan with begin and end dates for MMM yyyy" in {
    val dateStr = "May 2015"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-05"),
      begin = Some("2015-05"),
      end = Some("2015-05")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }

  // yyyy MMM
  it should "create a valid EdmTimeSpan with begin and end dates for yyyy-MMM" in {
    val dateStr = "2015-Mar"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-03"),
      begin = Some("2015-03"),
      end = Some("2015-03")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }

  it should "create a valid EdmTimeSpan with begin and end dates for yyyy/MMM" in {
    val dateStr = "2015/Mar"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-03"),
      begin = Some("2015-03"),
      end = Some("2015-03")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }

  it should "create a valid EdmTimeSpan with begin and end dates for yyyy MMM" in {
    val dateStr = "2015 Mar"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-03"),
      begin = Some("2015-03"),
      end = Some("2015-03")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }

  // yyyy MM
  it should "create a valid EdmTimeSpan with begin and end dates for yyyy-MM" in {
    val dateStr = "2015-03"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-03"),
      begin = Some("2015-03"),
      end = Some("2015-03")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }

  it should "create a valid EdmTimeSpan with begin and end dates for yyyy/MM" in {
    val dateStr = "2015/03"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-03"),
      begin = Some("2015-03"),
      end = Some("2015-03")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }

  it should "create a valid EdmTimeSpan with begin and end dates for yyyy MM" in {
    val dateStr = "2015 03"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015-03"),
      begin = Some("2015-03"),
      end = Some("2015-03")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }

  // yyyy
  it should "create a valid EdmTimeSpan with begin and end dates for yyyy" in {
    val dateStr = "2015"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = Some("2015"),
      begin = Some("2015"),
      end = Some("2015")
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }

  // Anti-tests
  // 1993-1994
  it should "do nothing with 1993-1994; " in {
    val dateStr = "1993-1994; "
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = None,
      begin = None,
      end = None
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }
  // 1975/1985
  it should "do nothing with 1975/1985" in {
    val dateStr = "1975/1985"
    val expectedDate = EdmTimeSpan(
      originalSourceDate = Some(dateStr),
      prefLabel = None,
      begin = None,
      end = None
    )
    assert(dateBuilder.buildEdmTimeSpan(dateStr) === expectedDate)
  }
}
