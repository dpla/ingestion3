package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}


class RumseyMappingTest extends FlatSpec with BeforeAndAfter {

  val shortName = "rumsey"
  val xmlString: String = new FlatFileIO().readFileAsString("/rumsey.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new RumseyMapping

  it should "extract the correct originalId" in {
    val expected = Some("oai:N/A:RUMSEY~8~1~318428~90087368")
    assert(extractor.originalId(xml) == expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("David Rumsey").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) == expected)
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq("https://www.davidrumsey.com/luna/servlet/detail/RUMSEY~8~1~318428~90087368").map(stringOnlyWebResource)
    assert(extractor.isShownAt(xml) == expected)
  }

  it should "extract the correct contributor" in {
    val expected = Seq("Name of contributor").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("Burrard, Sidney Gerald", "Survey of India").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct date" in {
    val expected = Seq("1912", "1912").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) == expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("Description")
    assert(extractor.description(xml) == expected)
  }

  it should "extract the correct identifier" in {
    val expected = Seq("https://www.davidrumsey.com/luna/servlet/detail/RUMSEY~8~1~318428~90087368",
    "https://www.davidrumsey.com/rumsey/Size1/RUMSEY~8~1/179/10403000.jpg")
    assert(extractor.identifier(xml) == expected)
  }

  it should "extract the correct place" in {
    val expected = Seq("Kolkata (India)", "Calcutta (India)", "Calcutta").map(nameOnlyPlace)
    assert(extractor.place(xml) == expected)
  }

  it should "extract the correct subject" in {
    val expected = Seq("Subject").map(nameOnlyConcept)
    assert(extractor.subject(xml) == expected)
  }

  it should "extract the correct title" in {
    val expected = Seq(
      "City of Calcutta. Published under the direction of Colonel S.G. Burrard, R.E.F.R.S.. Officiating Surveyor General of India. September 1911. With additions and corrections to April 1912.",
      "City of Calcutta")
    assert(extractor.title(xml) == expected)
  }

  it should "extract the correct type" in {
    val expected = Seq("image")
    assert(extractor.`type`(xml) == expected)
  }

  it should "extract the correct preview" in {
    val expected = Seq("https://www.davidrumsey.com/rumsey/Size1/RUMSEY~8~1/179/10403000.jpg")
      .map(stringOnlyWebResource)
    assert(extractor.preview(xml) == expected)
  }
}
