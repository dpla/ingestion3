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

  // Helpers
  def webResourceFor(url: String): EdmWebResource =
    EdmWebResource(uri = URI(url))

  // URI.hasBareIpHost tests
  it should "detect a bare IPv4 address as a bare-IP host" in {
    assert(URI("http://67.111.179.146/foo?bar=1").hasBareIpHost)
  }

  it should "detect a bare IPv6 address as a bare-IP host" in {
    assert(URI("http://[::1]/foo").hasBareIpHost)
  }

  it should "not flag a normal domain name as a bare-IP host" in {
    assert(!URI("https://digital.cjh.org/webclient/foo").hasBareIpHost)
  }

  it should "not flag an invalid URI as a bare-IP host" in {
    assert(!URI("not a url").hasBareIpHost)
  }

  // validateIsShownAt bare-IP tests
  it should "log an ERROR when isShownAt has a bare-IP URL" in {
    msgCollector.deleteAll()
    val url = "http://67.111.179.146/item/123"
    val values = Seq(webResourceFor(url))
    val expected = bareIpUrlMsg(id, "isShownAt", url, enforce = true)

    mapTest.validateIsShownAt(values, id, enforce = true)

    assert(msgCollector.getAll.contains(expected))
  }

  it should "still return the resource when isShownAt has a bare-IP URL" in {
    msgCollector.deleteAll()
    val url = "http://67.111.179.146/item/123"
    val values = Seq(webResourceFor(url))
    val result = mapTest.validateIsShownAt(values, id, enforce = true)
    assert(result.uri.value === url)
  }

  // validateObject bare-IP tests
  it should "log a WARN and return None when object has a bare-IP URL" in {
    msgCollector.deleteAll()
    val url = "http://67.111.179.146/thumb/123"
    val values = Seq(webResourceFor(url))
    val expected = bareIpUrlMsg(id, "object", url, enforce = false)

    val result = mapTest.validateObject(values, id, enforce = false)

    assert(msgCollector.getAll.contains(expected))
    assert(result === None)
  }

  it should "return the resource when object has a valid domain URL" in {
    msgCollector.deleteAll()
    val url = "https://digital.cjh.org/thumb/123"
    val values = Seq(webResourceFor(url))
    val result = mapTest.validateObject(values, id, enforce = false)
    assert(result.isDefined)
    assert(result.get.uri.value === url)
  }

  // validatePreview bare-IP tests
  it should "log a WARN and return None when preview has a bare-IP URL" in {
    msgCollector.deleteAll()
    val url = "http://10.0.0.1/preview/123"
    val values = Seq(webResourceFor(url))
    val expected = bareIpUrlMsg(id, "preview", url, enforce = false)

    val result = mapTest.validatePreview(values, id, enforce = false)

    assert(msgCollector.getAll.contains(expected))
    assert(result === None)
  }

  // validateHasView bare-IP tests
  it should "filter out bare-IP entries from hasView and log a WARN for each" in {
    msgCollector.deleteAll()
    val badUrl = "http://67.111.179.146/view/123"
    val goodUrl = "https://digital.cjh.org/view/456"
    val values = Seq(webResourceFor(badUrl), webResourceFor(goodUrl))
    val expected = bareIpUrlMsg(id, "hasView", badUrl, enforce = false)

    val result = mapTest.validateHasView(values, id)

    assert(msgCollector.getAll.contains(expected))
    assert(result.size === 1)
    assert(result.head.uri.value === goodUrl)
  }

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
