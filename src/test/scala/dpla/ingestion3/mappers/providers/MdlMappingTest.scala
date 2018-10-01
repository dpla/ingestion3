package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JString
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FlatSpec}

class MdlMappingTest extends FlatSpec with BeforeAndAfter {

  val shortName = "mdl"
  val jsonString: String = new FlatFileIO().readFileAsString("/mdl.json")
  val json: Document[JValue] = Document(parse(jsonString))
  val extractor = new MdlMapping

  it should "extract correct edmRights" in {
    val expected = Some(new URI("http://rightsstatements.org/vocab/InC-EDU/1.0/"))

    assert(extractor.edmRights(json) === expected)
  }
}