package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

class OrbisCascadeMappingTest extends AnyFlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "orbis-cascade"
  val xmlString: String = new FlatFileIO().readFileAsString("/orbis-cascade.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new OrbisCascadeMapping

  it should "use the provider shortname in minting IDs "in
    assert(extractor.useProviderName)

  it should "extract the correct original identifier " in
    assert(extractor.originalId(xml) === Some("http://harvester.orbiscascade.org/record/e466e93cf4849fd8aa36f1daa1417c15"))

  it should "extract the correct contributor" in {
    val expected = Seq("University of Washington Libraries, Special Collections").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  // tbd creator

  it should "extract the correct dates" in {
    val expected = Seq(
      EdmTimeSpan(
        originalSourceDate=Some("1903-1906"),
        begin = Some("1903"),
        end = Some("1906")
      ))
    assert(extractor.date(xml) === expected)
  }

  it should "extract the correct description" in
    assert(extractor.description(xml) == Seq("PH Coll 41.96c Scanned from an original photographic print at 110 dpi in JPEG format at compression rate 3 and resized to 768x600 ppi. 2015"))

  it should "extract the correct place" in {
    val expected = Seq("Canada--Yukon Territory").map(nameOnlyPlace)
    assert(extractor.place(xml) === expected)
  }


  it should "extract the correct language" in {
    val expected = Seq("eng").map(nameOnlyConcept)
    assert(extractor.language(xml) === expected)
  }

  it should "extract edmRights value if it is a URI'" in {
    val expected = Seq(URI("http://rightsstatements.org/vocab/CNE/1.0/"))
    assert(extractor.edmRights(xml) === expected)
  }

  it should "extract rights value it is text'" in {
    val expected = Seq("For information on permissions for use and reproductions please visit UW Libraries Special Collections Use Permissions page:   http://www.lib.washington.edu/specialcollections/services/permission-for-use")
    assert(extractor.rights(xml) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq("Mountains--Yukon").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("View of opposite shore from water with mountain in distance, Yukon Territory, circa 1903-1906")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct types" in {
    val expected = Seq("Photograph")
    assert(extractor.`type`(xml) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("University of Idaho Library, Special Collections and Archives").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) === expected)
  }

  it should "extract isShownAt from both possible locations" in {
    val expected = Seq(
      "http://cdm16786.contentdm.oclc.org/cdm/ref/collection/alaskawcanada/id/7049"
//      ,"http://cdm16786.contentdm.oclc.org/utils/getstream/collection/alaskawcanada/id/7049",
//      "http://cdm16786.contentdm.oclc.org/utils/getstream/collection/sayre/id/11790"
    ).map(stringOnlyWebResource)
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "apply a nwdh tag" in {
    val expected = Seq(URI("nwdh"))
    assert(extractor.tags(xml) === expected)
  }
}