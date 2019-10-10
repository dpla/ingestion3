package dpla.ingestion3.mappers

import dpla.ingestion3.mappers.utils.{Document, Mapping, XmlMapping}
import dpla.ingestion3.messages.{IngestMessage, IngestMessageTemplates, MessageCollector}
import dpla.ingestion3.model.{OreAggregation, _}
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.NodeSeq


class MapperTest extends FlatSpec with BeforeAndAfter with IngestMessageTemplates {

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
    val message = moreThanOneValueMsg(id, "dataProvider", "Person A | Person B", msg = None, enforce)
    val validatedDataProvider = mapTest.validateDataProvider(dataProviders, id, enforce)

    assert(msgCollector.getAll().contains(message))
    assert(validatedDataProvider === dataProviders.head)
  }

  it should "validate edmRights by checking size <= 1 and values are valid edmRights URIs" in {
    val rightsUri = Seq("http://rightsstatements.org", "http://creativecommons.org", "dub dub dub rights.com")
    val message = mintUriMsg(id, "edmRights", "dub dub dub rights.com", msg = None, enforce)
    val rights = rightsUri.map(URI)

    val validatedEdmRights = mapTest.validateEdmRights(rights, id, enforce)

    assert(msgCollector.getAll().contains(message))
    assert(validatedEdmRights === rights.headOption)
  }

  it should "log an error when both emdRights and rights are empty" in {
    val rights = Seq()
    val edmRight = Seq()
    val message = missingRights(id, enforce)

    mapTest.validateRights(edmRight, rights, id, enforce)
    assert(msgCollector.getAll().contains(message))
  }
}
