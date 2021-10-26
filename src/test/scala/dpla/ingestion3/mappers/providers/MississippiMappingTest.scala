package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.utils.FlatFileIO
import dpla.ingestion3.model._
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

class MississippiMappingTest extends AnyFlatSpec with BeforeAndAfter {

  val shortName = "mississippi"
  val jsonString: String = new FlatFileIO().readFileAsString("/ms.json")
  val json: Document[JValue] = Document(parse(jsonString))
  val extractor = new MississippiMapping

  it should "use the provider short name when minting DPLA ids" in {
    assert(extractor.useProviderName === true)
  }

  it should "return the correct provider name" in {
    assert(extractor.getProviderName === Some("mississippi"))
  }

  it should "extract the correct original ID" in {
    val expected = Some(
      "https://na04.alma.exlibrisgroup.com/primaws/rest/pub/pnxs/L/991014100609305566"
    )
    assert(extractor.originalId(json) === expected)
  }

  it should "extract the correct previews" in {
    val expected = Seq(
      "http://cdm16631.contentdm.oclc.org/utils/getthumbnail/collection/p16631coll10/id/3729"
    )
      .map(stringOnlyWebResource)
    assert(extractor.preview(json) === expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("eng").map(nameOnlyConcept)
    assert(extractor.language(json) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq("Piano").map(nameOnlyConcept)
    assert(extractor.subject(json) === expected)
  }

  it should "extract the correct title" in {
    val expected = Seq("My Ramapoo")
    assert(extractor.title(json) === expected)
  }

  it should "extract the correct formats" in {
    val expected = Seq("1 score; (6 p.)", "JPEG")
    assert(extractor.format(json) === expected)
  }

  it should "extract the correct description" in {
    val expected = Seq(
      "Illustration of Native American girl sitting and Native American man on  horse. Forest and teepees"
    )
    assert(extractor.description(json) === expected)
  }

  it should "extract the correct rights" in {
    val expected = Seq("Item Unavailable Due to Copyright Restrictions")
    assert(extractor.rights(json) === expected)
  }

  it should "extract the correct identifiers" in {
    val expected = Seq(
      "011491515-1910;32278011491515;Box 85; Folder 1; Piece 7;digital_TIF_archive\\Templeton_master\\Folder_85-1;http://cdm16631.contentdm.oclc.org/cdm/ref/collection/p16631coll10/id/3729;oai:cdm16631.contentdm.oclc.org:p16631coll10/3729"
    )
    assert(extractor.identifier(json) === expected)
  }

  it should "extract the correct dates" in {
    val expected = Seq("1910").map(stringOnlyTimeSpan)
    assert(extractor.date(json) === expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("Hager, Frederick W., 1874-1958").map(nameOnlyAgent)
    assert(extractor.creator(json) === expected)
  }

  it should "extract the correct contributor" in {
    val expected = Seq(
      "Starmer, William A. (William Austin), b. 1872; Starmer, Frederick S., b. 1879"
    ).map(nameOnlyAgent)
    assert(extractor.contributor(json) === expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq(
      "New York : The Jos. Morris Co.",
      "Mississippi State University Libraries (electronic version)"
    ).map(nameOnlyAgent)
    assert(extractor.publisher(json) === expected)
  }

}
