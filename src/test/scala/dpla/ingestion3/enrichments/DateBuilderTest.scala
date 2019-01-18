package dpla.ingestion3.enrichments

import dpla.ingestion3.enrichments.date.DateBuilder
import dpla.ingestion3.model.EdmTimeSpan
import org.scalatest._
import org.scalatest.prop.Checkers

class DateBuilderTest extends FlatSpec with BeforeAndAfter with Matchers with Checkers {

  val dateBuilder = new DateBuilder()

   it should "create a valid EdmTimeSpan with begin and end dates for 2015" in {
     val dateStr = Some("2015")
     val expectedDate = EdmTimeSpan(
       originalSourceDate = dateStr,
       prefLabel = dateStr ,
       begin = dateStr ,
       end = dateStr
     )
     assert(dateBuilder.generateBeginEnd(dateStr) === expectedDate)
   }

  it should "create a valid EdmTimeSpan with begin and end dates for 2015-2017" in {
    val dateStr = Some("2015-2017")
    val expectedDate = EdmTimeSpan(
      originalSourceDate = dateStr,
      prefLabel = dateStr,
      begin = Some("2015"),
      end = Some("2017")
    )
    assert(dateBuilder.generateBeginEnd(dateStr) === expectedDate)
  }
}
