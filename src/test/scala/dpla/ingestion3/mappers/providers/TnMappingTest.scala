package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class TnMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "tn"
  val xmlString: String = new FlatFileIO().readFileAsString("/tn.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new TnMapping

  // provider prefix
  it should "not use provider prefix when minting IDs" in {
    assert(extractor.useProviderName() === false)
  }

  // alternate title
  it should "extract the correct alternate title" in {
    val expected = Seq("alt title 1")
    assert(extractor.alternateTitle(xml) == expected)
  }

  // collection
  it should "extract the correct collections " in {
    val expected = Seq(
      DcmiTypeCollection(title = Some("project title"), description = Some("project description")),
      DcmiTypeCollection(title = Some("collection title")))
    assert(extractor.collection(xml) == expected)
  }

  // contributor
  it should "extract the correct contributors" in {
    val expected = Seq("Boston Cooking School (Boston, Mass.)")
      .map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  // creator
  it should "extract the correct creators" in {
    val expected = Seq("City of Lansing", "City Planning Division Ingells, Norris").map(nameOnlyAgent)
    assert(extractor.creator(xml) === expected)
  }

  // date
  it should "extract the correct date" in {
    val expected = Seq("1877").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) === expected)
  }

  // description
  it should "extract the correct description" in {
    val expected = List("Description")
    assert(extractor.description(xml) === expected)
  }

  // extent
  it should "extract the correct extent" in {
    val expected = List("Extent")
    assert(extractor.extent(xml) === expected)
  }

  // format
  it should "extract the correct format" in {
    val expected = List("Format")
    assert(extractor.format(xml) === expected)
  }

  // identifier
  it should "extract the correct identifier" in {
    val expected = List("33783")
    assert(extractor.identifier(xml) === expected)
  }

  // language
  it should "extract the correct language" in {
    val expected = Seq("eng").map(nameOnlyConcept)
    assert(extractor.language(xml) == expected)
  }

  // place
  it should "extract the correct place" in {
    val expected = List(DplaPlace(name = Some("geographic 1"), coordinates = Some("lat/long 1")),
      nameOnlyPlace("Maury County (Tenn.)"))
    assert(extractor.place(xml) == expected)
  }

  // publisher
  it should "extract the correct publisher" in {
    val expected = List("Publisher").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  // relation
  it should "extract the correct relation" in {
    val expected = List("Relation title", "Relation url").map(eitherStringOrUri)
    assert(extractor.relation(xml) === expected)
  }

  // replacedBy
  it should "extract the correct replacedBy" in {
    val expected = List("isReferencedBy title", "isReferencedBy url")
    assert(extractor.replacedBy(xml) === expected)
  }

  // replaces
  it should "extract the correct replaces" in {
    val expected = List("replaces title", "replaces url")
    assert(extractor.replaces(xml) === expected)
  }

  // rights
  it should "extract the correct rights" in {
    val expected = List("Rights statement.")
    assert(extractor.rights(xml) === expected)
  }

  // subject
  it should "extract the correct subject" in {
    val expected = List(
      "Agriculture",
      "Killebrew, J. B. (Joseph Buckner), 1831-1906",
      "Maury County (Tenn.)").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  // temporal
  it should "extract the correct temporal" in {
    val expected = List("Temporal").map(stringOnlyTimeSpan)
    assert(extractor.temporal(xml) === expected)
  }

  // title
  it should "extract the correct title" in {
    val expected = List("Title", "County Manufacturing Statistics for Maury County")
    assert(extractor.title(xml) === expected)
  }

  // type
  it should "extract the correct type" in {
    val expected = List("text", "still image")
    assert(extractor.`type`(xml) === expected)
  }

  // dataProvider
  it should "extract the correct dataProvider" in {
    val expected = List("Tennessee State Library and Archives").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) === expected)
  }

  // edmRights
  it should "extract the correct edmRights" in {
    val expected = List(URI("http://rightsstatements.org/vocab/InC-EDU/1.0/"))
    assert(extractor.edmRights(xml) === expected)
  }

  // intermediateProvider
  it should "extract the correct intermediateProvide" in {
    val expected = Some(nameOnlyAgent("Intermediate Provider"))
    assert(extractor.intermediateProvider(xml) === expected)
  }

  // isShownAt
  it should "extract the correct isShownAt" in {
    val expected = List("http://cdm15138.contentdm.oclc.org/cdm/ref/collection/agricult/id/62")
      .map(stringOnlyWebResource)
    assert(extractor.isShownAt(xml) === expected)
  }

  // object
  it should "extract the correct object" in {
    val expected = List("http://cdm15138.contentdm.oclc.org/utils/getthumbnail/collection/agricult/id/62")
      .map(stringOnlyWebResource)
    assert(extractor.`object`(xml) === expected)
  }

  // preview
  it should "extract the correct preview" in {
    val expected = List("http://cdm15138.contentdm.oclc.org/utils/getthumbnail/collection/agricult/id/62")
      .map(stringOnlyWebResource)
    assert(extractor.preview(xml) === expected)
  }

}
