package dpla.ingestion3.mappers

import dpla.ingestion3.mappers.utils.{Document, Mapping, XmlMapping}
import dpla.ingestion3.messages.{IngestMessage, IngestMessageTemplates, MessageCollector}
import dpla.ingestion3.model.{OreAggregation, _}
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.NodeSeq


class MapperTest extends AnyFlatSpec with BeforeAndAfter with IngestMessageTemplates {

  val enforce = false

  class MapTest extends Mapper[NodeSeq, XmlMapping] {
    // Stubbed implementation
    override def map(document: Document[NodeSeq], mapping: Mapping[NodeSeq]): OreAggregation =
      emptyOreAggregation
  }

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val mapTest = new MapTest
  val dataProviders: Seq[EdmAgent] = Seq("Person A", "Person B").map(nameOnlyAgent)
  val id = "123"

  it should "add an info warning if more than two dataProvider are given and return the first value" in {
    msgCollector.deleteAll()
    val message = moreThanOneValueMsg(id, "dataProvider", "Person A | Person B", enforce)
    val validatedDataProvider = mapTest.validateDataProvider(dataProviders, id, enforce)

    assert(msgCollector.getAll.contains(message))
    assert(validatedDataProvider === dataProviders.head)
  }

  it should "map the first valid rights URI" in {

    val rightsUris = Seq(
      "http://rightsstatements.org/vocab/CNE/1.0/",
      "http://creativecommons.org/licenses/by-nc-nd/1.0/",
      "dub dub dub rights.com"
    ).map(URI)

    val validatedEdmRights = mapTest.validateEdmRights(rightsUris, id, enforce)

    assert(validatedEdmRights === rightsUris.headOption)
  }

  it should "map normalize a rights URI by trimming extra whitespace" in {

    val rightsUris = Seq("http://rightsstatements.org/vocab/CNE/1.0/ ").map(URI)
    val normalizedRightsUri = Seq(URI("http://rightsstatements.org/vocab/CNE/1.0/"))

    val validatedEdmRights = mapTest.normalizeEdmRights(rightsUris, id)

    assert(validatedEdmRights === normalizedRightsUri)
  }

  it should "normalized a rights URI by removing /rdf from path" in {

    val rightsUris = Seq("http://creativecommons.org/licenses/by/4.0/rdf").map(URI)
    val normalizedRightsUri = Seq(URI("http://creativecommons.org/licenses/by/4.0/"))

    val validatedEdmRights = mapTest.normalizeEdmRights(rightsUris, id)

    assert(validatedEdmRights === normalizedRightsUri)
  }

  it should "log a warning when an invalid rs.org value is provided and enforce is FALSE" in {
    msgCollector.deleteAll()
    val rightsString = "http://rightsstatements.org/"
    val rightsUris = Seq(rightsString).map(URI)

    mapTest.validateEdmRights(rightsUris, id, enforce = false)
    val msg = invalidEdmRightsMsg(id, "edmRights", rightsString, enforce = false)

    assert(msgCollector.getAll.contains(msg))
  }

  it should "log a message when it normalized a edmRights value and enforce = FALSE" in {
    msgCollector.deleteAll()
    val rightsString = "https://rightsstatements.org/vocab/CNE/1.0/"
    val rightsUris = Seq(rightsString).map(URI)
    val message = normalizedEdmRightsHttpsMsg(id, "edmRights", rightsString, enforce = false)

    mapTest.normalizeEdmRights(rightsUris, id)
    
    assert(msgCollector.getAll.contains(message))
  }


  it should "log an ERROR when both emdRights and rights are empty and enforce = TRUE" in {
    msgCollector.deleteAll()
    val rights = Seq()
    val edmRights = None
    val message = missingRights(id, enforce = true)

    mapTest.validateRights(rights, edmRights, id, enforce = true)
    assert(msgCollector.getAll.contains(message))
  }

  it should "not normalize a uri that is invalid because it contains a whitespace in the path and " +
    "return the original value" in {
    msgCollector.deleteAll()
    val rightsString = "http://rightsstatements.org/vocab%20/UND/1.0/"
    val rightsUris = Seq(rightsString).map(URI)
    val normalizedRights = mapTest.normalizeEdmRights(rightsUris, id)
    assert(normalizedRights === rightsUris)
  }

  it should "log a missingRights ERROR when dcRights is empty, edmRights is invalid and " +
    "enforce missing rights = TRUE for validateRights and enforce for validateEdmRights = FALSE" in {
    msgCollector.deleteAll()
    val missingRightsErrorMsg = missingRights(id, enforce = true)
    val dcRights = Seq()
    val rightsString = "https://rightsstatements.org/"
    val rightsUris = Seq(rightsString).map(URI)

    // normalize and validate edmRights value
    val normalizedRights = mapTest.normalizeEdmRights(rightsUris, id)
    val edmRights = mapTest.validateEdmRights(normalizedRights, id, enforce = false)

    mapTest.validateRights(dcRights, edmRights, id, enforce = true)

    assert(msgCollector.getAll.contains(missingRightsErrorMsg))
    assert(edmRights === None)
  }

  it should "not log a missingRights ERROR when dcRights is present, edmRights is invalid and " +
    "enforce missing rights = TRUE for validateRights and enforce for validateEdmRights = FALSE" in {
    msgCollector.deleteAll()
    val missingRightsErrorMsg = missingRights(id, enforce = true)
    val dcRights = Seq("Freetext Friday")
    val rightsString = "https://rightsstatements.org/"
    val rightsUris = Seq(rightsString).map(URI)

    // normalize and validate edmRights value
    val normalizedRights = mapTest.normalizeEdmRights(rightsUris, id)
    val edmRights = mapTest.validateEdmRights(normalizedRights, id, enforce = false)

    mapTest.validateRights(dcRights, edmRights, id, enforce = true)

    assert(!msgCollector.getAll.contains(missingRightsErrorMsg))
    assert(edmRights === None)
  }
}
