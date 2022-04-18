package dpla.eleanor

import dpla.eleanor.mappers.StandardEbooksMapping
import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model.{EdmAgent, EdmWebResource, SkosConcept, URI, nameOnlyConcept}
import dpla.ingestion3.utils.Utils
import org.apache.commons.io.IOUtils
import org.scalatest.FlatSpec

import scala.io.Source
import scala.xml.{NodeSeq, XML}
import org.json4s.JsonDSL._

class StandardEbooksMappingTest extends FlatSpec {

  val mapping = new StandardEbooksMapping()

  /**
    * Reads a file and returns it as a single string
    *
    * @param name
    * @return
    */
  def readFileAsString(name: String): String = {
    val stream = getClass.getResourceAsStream(name)
    val result = Source.fromInputStream(stream).mkString
    IOUtils.closeQuietly(stream)
    result
  }

  val xmlString: String = readFileAsString("/standardebooks.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))

  "StandardEbooks" should "map the correct title" in {
    val expected = Seq("Robbery Under Arms")
    assert(mapping.title(xml) === expected)
  }

  it should "extract the correct original id" in {
    val expected = "https://standardebooks.org/ebooks/rolf-boldrewood/robbery-under-arms"
    assert(mapping.originalId(xml) === Some(expected))
  }

  it should "have the correct short name" in {
    val expected = "standard-ebooks"
    assert(mapping.getProviderName === Some(expected))
  }

  it should "map the correct creator" in {
    val expected = EdmAgent(
      name = Some("Rolf Boldrewood"),
      providedLabel = Some("Rolf Boldrewood"),
      exactMatch = Seq(
        URI("https://en.wikipedia.org/wiki/Rolf_Boldrewood"),
        URI("http://id.loc.gov/authorities/names/n50042832")
      )
    )
    assert(mapping.creator(xml) === Seq(expected))
  }

  it should "map the correct language" in {
    val expected = Seq(nameOnlyConcept("en-AU"))
    assert(mapping.language(xml) === expected)
  }

  it should "map the correct description" in {
    val expected = Seq("A young Australian man and his brother are led into a life of crime, becoming cattle stealers, bank robbers and bushrangers before being caught.")
    assert(mapping.description(xml) === expected)
  }

  it should "map the correct subject" in {
    val expected = Seq(
      "Frontier and pioneer life -- Australia -- Fiction",
      "Gold mines and mining -- Australia -- Fiction",
      "Bushrangers -- Australia -- Fiction",
      "Australia -- Gold discoveries -- Fiction"
    ).map(text => SkosConcept(
      concept = Some(text),
      providedLabel = Some(text),
      scheme = Some(URI("http://purl.org/dc/terms/LCSH"))
    ))
    assert(mapping.subject(xml) === expected)
  }

  it should "map the correct rights" in {
    val expected = "Public domain in the United States; original content " +
      "released to the public domain via the Creative Commons CC0 1.0 " +
      "Universal Public Domain Dedication"

    assert(mapping.rights(xml) === Seq(expected))
  }

  it should "map the correct publisher" in {
    val expected = mapping.provider(xml)
    assert(mapping.publisher(xml) === Seq(expected))
  }

  it should "map the correct type" in {
    assert(mapping.`type`(xml) === Seq("text"))
  }

  it should "map the correct format" in {
    assert(mapping.format(xml) === Seq("Ebook"))
  }

  it should "map the correct isShownAt" in {
    val expected = Seq(EdmWebResource(
      uri = URI("https://standardebooks.org/ebooks/rolf-boldrewood/robbery-under-arms")
    ))
    assert(mapping.isShownAt(xml) === expected)
  }

  it should "map the correct preview" in {
    val expected = Seq(EdmWebResource(
      uri = URI("http://standardebooks.org/ebooks/rolf-boldrewood/robbery-under-arms/downloads/cover-thumbnail.jpg")
    ))
    assert(mapping.preview(xml) === expected)
  }

  it should "map the correct sidecar" in {
    val expected =
      ("prehashId" -> "standard-ebooks--https://standardebooks.org/ebooks/rolf-boldrewood/robbery-under-arms") ~
        ("dplaId" -> "fa10a7a586b209a62bfdf9999b52c2ce")
    assert(mapping.sidecar(xml) === expected)
  }

  it should "map the correct originalRecord" in {
    val expected = Utils.formatXml(xml.value)
    assert(mapping.originalRecord(xml) === expected)
  }
}
