
package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter

class CommunityWebsMappingTest extends AnyFlatSpec with BeforeAndAfter {

  val shortName = Some("community-webs")
  val jsonString: String = new FlatFileIO().readFileAsString("/community-webs.json")
  val json: Document[JValue] = Document(parse(jsonString))
  val extractor = new CommunityWebsMapping


  it should "use the provider short name when minting DPLA ids" in {
    assert(extractor.useProviderName === true)
  }

  it should "return the correct provider name" in {
    assert(extractor.getProviderName === shortName)
  }

  it should "extract the correct original ID" in {
    val expected = Some("archive-it::seed:2225815")
    assert(extractor.originalId(json) === expected)
  }
}
