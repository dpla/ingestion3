package dpla.ingestion3.mappers.providers

import java.net.URI

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
//  it should "extract provider id" in {
//    val expected = "https://plains2peaks.org/0a00aa9c-0b9e-11e8-a081-005056c00008"
//
//    val id = (json.get \ "@graph").children.flatMap(elem => {
//      elem \ "@type" match {
//        case JString("ore:Aggregation") => Some(elem \ "@id")
//        case _ => None
//      }
//    }).headOption
//    println(id)
//
//    assert(extractor.getProviderId(json) === expected)
//  }
//  it should "extract alternate titles" in {
//    val expected = Seq("Alt title 1")
//    assert(extractor.alternateTitle(json) === expected)
//  }
//  it should "extract contributor" in {
//    val expected = Seq("Cobos, Ruben").map(nameOnlyAgent)
//    assert(extractor.contributor(json) === expected)
//  }
//  it should "extract creator" in {
//    val expected = Seq("Berry, Edwin").map(nameOnlyAgent)
//    assert(extractor.creator(json) === expected)
//  }
//  it should "extract date" in {
//    val expected = Seq("20th century").map(stringOnlyTimeSpan)
//    assert(extractor.date(json) === expected)
//  }
//  it should "extract description" in {
//    val expected = Seq("Tome, New Mexico. No listing in Cobos index. Quality: good/fair. PLEASE NOTE: this should be number 15 of 17 songs on the audiofile.")
//    assert(extractor.description(json) === expected)
//  }
}