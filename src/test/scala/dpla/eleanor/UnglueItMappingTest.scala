package dpla.eleanor

import dpla.eleanor.mappers.UnglueItMapping
import org.apache.commons.io.IOUtils
import org.scalatest.FlatSpec

import scala.io.Source
import scala.xml.{Elem, XML}

class UnglueItMappingTest extends FlatSpec {

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

  val xmlString: String = readFileAsString("/unglueit.xml")
  val xml: Elem = XML.loadString(xmlString)

  "Unglueit" should "map the correct title" in {
    val expected = Seq("Römische Elegien")
    assert(UnglueItMapping.title(xml) === expected)
  }

  it should "map the correct author" in {
    val expected = Seq("Johann Wolfgang von Goethe")
    assert(UnglueItMapping.author(xml) === expected)
  }

  it should "map the correct language" in {
    val expected = Seq("de")
    assert(UnglueItMapping.language(xml) === expected)
  }

  it should "map the correct summary" in {
    val expected = Seq("Summary of books")
    assert(UnglueItMapping.summary(xml) === expected)
  }

  it should "map the correct genre" in {
    val expected = Seq("German poetry -- 18th century", "GITenberg", "PT")
    assert(UnglueItMapping.genre(xml) === expected)
  }

  it should "map the correct issued date" in {
    val expected = Seq("2004")
    assert(UnglueItMapping.publicationDate(xml) === expected)
  }
}
