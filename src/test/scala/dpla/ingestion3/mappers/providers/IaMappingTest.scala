package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST.{JNull, JObject}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FlatSpec}

class IaMappingTest extends FlatSpec with BeforeAndAfter {
  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "ia"
  val jsonString: String = new FlatFileIO().readFileAsString("/ia.json")
  val json: Document[JValue] = Document(parse(jsonString))
  val extractor = new IaMapping

  it should "extract the correct rights" in {
    val expected = Seq("Access to the Internet Archive’s Collections is granted for scholarship " +
    "and research purposes only. Some of the content available through the Archive may be governed " +
      "by local, national, and/or international laws and regulations, and your use of such content " +
      "is solely at your own risk")
    assert(extractor.rights(json) === expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq(nameOnlyAgent("Elms College"))
    assert(extractor.dataProvider(json) === expected)
  }

  it should "have a default dataProvider" in {
    val expected = Seq(nameOnlyAgent("Internet Archive"))

    val cloneJson = json.get.asInstanceOf[JObject].merge(JObject(("contributor", JNull)))

    assert(extractor.dataProvider(Document[JValue](cloneJson)) === expected)

  }

  it should "extract the correct original id" in {
    val expected = Some("artofdyingwell00bell")
    assert(extractor.originalId(json) == expected)
  }

  it should "extract the correct URL for isShownAt" in {
    val expected = Seq(stringOnlyWebResource("http://www.archive.org/details/artofdyingwell00bell"))
    assert(extractor.isShownAt(json) === expected)
  }

  it should "extract the correct url for preview" in {
    val expected = Seq(stringOnlyWebResource("https://archive.org/services/img/artofdyingwell00bell"))
    assert(extractor.preview(json) === expected)
  }

  it should "extract the correct date" in {
    val expected = Seq("1720-01-01T00:00:00Z").map(stringOnlyTimeSpan)
    assert(extractor.date(json) == expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("With a final errata leaf; \"Publish'd for the benefit of the translatour\"; M.E. Barry Rare Book Collection. Special Collections, Alumnae Library, Elms College; Description based on print version record.")
    assert(extractor.description(json) == expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("eng").map(nameOnlyConcept)
    assert(extractor.language(json) == expected)
  }

  it should "extract the correct publisher" in {
    val expected = Seq("Printed by I. Dalton. The Book may be had at Mr. Colstons, Mr. Jones, at Mr. Sunderland's coffee-house, Pilgrims coffee-house, Mrs. Whites")
      .map(nameOnlyAgent)
    assert(extractor.publisher(json) == expected)
  }

  it should "extract the correct subject" in {
    val expected = Seq("Death--Religious aspects--Catholic Church--Early works to 1800", "Christian life" )
      .map(nameOnlyConcept)
    assert(extractor.subject(json) == expected)
  }

  it should "extract the correct title" in {
    val expected = Seq("The art of dying well")
    assert(extractor.title(json) == expected)
  }

  it should "extract the correct title and vol" in {
    val json = org.json4s.jackson.JsonMethods.parse(
      """{
        |  "volume" : "vol 1.",
        |  "title" : "The art of dying well"
        |  }
      """.stripMargin)
    val expected = Seq("The art of dying well, vol 1.")
    assert(extractor.title(Document(json)) == expected)
  }

  it should "extract the correct title and vol and issue" in {
    val json = org.json4s.jackson.JsonMethods.parse(
      """{
        |  "issue" : "issue",
        |  "volume" : "vol 1.",
        |  "title" : "The art of dying well"
        |  }
      """.stripMargin)
    val expected = Seq("The art of dying well, vol 1., issue")
    assert(extractor.title(Document(json)) == expected)
  }

  it should "extract the correct multiple title and vol and issue" in {
    val json = org.json4s.jackson.JsonMethods.parse(
      """{
        |  "issue" : "issue",
        |  "volume" : "vol 1.",
        |  "title" : ["The art of dying well", "Act 2"]
        |  }
      """.stripMargin)
    val expected = Seq("The art of dying well, vol 1., issue", "Act 2")
    assert(extractor.title(Document(json)) == expected)
  }

  it should "extract the correct multiple title and multiple vol and issue" in {
    val json = org.json4s.jackson.JsonMethods.parse(
      """{
        |  "issue" : "issue",
        |  "volume" : ["vol 1.", "vol 2."],
        |  "title" : ["The art of dying well", "Act 2"]
        |  }
      """.stripMargin)
    val expected = Seq("The art of dying well, vol 1., issue", "Act 2, vol 2.")
    assert(extractor.title(Document(json)) == expected)
  }

  it should "extract the correct multiple title and multiple vol and multiple issue" in {
    val json = org.json4s.jackson.JsonMethods.parse(
      """{
        |  "issue" : ["issue 1", "issue 1"],
        |  "volume" : ["vol 1.", "vol 2."],
        |  "title" : ["The art of dying well", "Act 2"]
        |  }
      """.stripMargin)
    val expected = Seq("The art of dying well, vol 1., issue 1", "Act 2, vol 2., issue 1")
    assert(extractor.title(Document(json)) == expected)
  }

  it should "extract the correct type" in {
    val expected = Seq("texts")
    assert(extractor.`type`(json) == expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/a43347f17f5d153a56cc74f5ba3fc59b"))
    assert(extractor.dplaUri(json) === expected)
  }

  it should "extract the correct url for iiif manifest" in {
    val expected = Seq(URI("https://iiif.archivelab.org/iiif/artofdyingwell00bell/manifest.json"))
    assert(extractor.iiifManifest(json) === expected)
  }
 }
