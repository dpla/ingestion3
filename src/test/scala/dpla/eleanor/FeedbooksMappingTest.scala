package dpla.eleanor

import dpla.eleanor.mappers.FeedbooksMapping
import org.apache.commons.io.IOUtils
import org.scalatest.FlatSpec

import scala.io.Source
import scala.xml.{Elem, XML}

class FeedbooksMappingTest extends FlatSpec {

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

  val xmlString: String = readFileAsString("/feedbooks.xml")
  val xml: Elem = XML.loadString(xmlString)

  "Feedbooks" should "map the correct title" in {
    val expected = Seq("Maria: or, The Wrongs of Woman")
    assert(FeedbooksMapping.title(xml) === expected)
  }

  it should "map the correct author" in {
    val expected = Seq("Mary Wollstonecraft")
    assert(FeedbooksMapping.author(xml) === expected)
  }

  it should "map the correct language" in {
    val expected = Seq("en")
    assert(FeedbooksMapping.language(xml) === expected)
  }

  it should "map the correct summary" in {
    val expected = Seq("Wollstonecraft's philosophical and gothic novel revolves around the story of a woman imprisoned in an insane asylum by her husband. It focuses on the societal rather than the individual \"wrongs of woman\" and criticizes what Wollstonecraft viewed as the patriarchal institution of marriage in eighteenth-century Britain and the legal system that protected it. [Source: Wikipedia]")
    assert(FeedbooksMapping.summary(xml) === expected)
  }

  it should "map the correct genre" in {
    val expected = Seq("Fiction",
      "Literary")
    assert(FeedbooksMapping.genre(xml) === expected)
  }
}
