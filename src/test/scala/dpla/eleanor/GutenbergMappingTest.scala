package dpla.eleanor

import dpla.eleanor.mappers.GutenbergMapping
import org.apache.commons.io.IOUtils
import org.scalatest.FlatSpec

import scala.io.Source
import scala.xml.{Elem, XML}

class GutenbergMappingTest extends FlatSpec {

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

  val xmlString: String = readFileAsString("/gutenberg.xml")
  val xml: Elem = XML.loadString(xmlString)

  "Gutenberg" should "map the correct uri " in {
    val expected = Seq("https://www.gutenberg.org/ebooks/1")
    assert(GutenbergMapping.uri(xml) === expected)
  }
  it should "map the correct title" in {
    val expected = Seq("The Declaration of Independence of the United States of America")
    assert(GutenbergMapping.title(xml) === expected)
  }

  it should "map the correct author" in {
    val expected = Seq("Jefferson, Thomas")
    assert(GutenbergMapping.author(xml) === expected)
  }

  it should "map the correct language" in {
    val expected = Seq("en")
    assert(GutenbergMapping.language(xml) === expected)
  }

  it should "map the correct summary" in {
    val expected = Seq("This is the original PG edition.")
    assert(GutenbergMapping.summary(xml) === expected)
  }

  it should "map the correct genre" in {
    val expected = Seq("American Revolutionary War", "United States Law", "Politics")
    assert(GutenbergMapping.genre(xml) === expected)
  }
}
