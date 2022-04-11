package dpla.eleanor
import dpla.eleanor.mappers.StandardEbooksMapping
import org.apache.commons.io.IOUtils
import org.scalatest.FlatSpec

import scala.io.Source
import scala.xml.{Elem, XML}

class StandardEbooksMappingTest extends FlatSpec {

  /**
    * Reads a file and returns it as a single string
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
  val xml: Elem = XML.loadString(xmlString)

  "StandardEbooks" should "map the correct title" in {
    val expected = Seq("Robbery Under Arms")
    assert(StandardEbooksMapping.title(xml) === expected)
  }

  it should "map the correct author" in {
    val expected = Seq("Rolf Boldrewood")
    assert(StandardEbooksMapping.author(xml) === expected)
  }

  it should "map the correct language" in {
    val expected = Seq("en-AU")
    assert(StandardEbooksMapping.language(xml) === expected)
  }

  it should "map the correct summary" in {
    val expected = Seq("A young Australian man and his brother are led into a life of crime, becoming cattle stealers, bank robbers and bushrangers before being caught.")
    assert(StandardEbooksMapping.summary(xml) === expected)
  }

  it should "map the correct genre" in {
    val expected = Seq("Frontier and pioneer life -- Australia -- Fiction",
      "Gold mines and mining -- Australia -- Fiction",
      "Bushrangers -- Australia -- Fiction",
      "Australia -- Gold discoveries -- Fiction")
    assert(StandardEbooksMapping.genre(xml) === expected)
  }
}
