package dpla.ingestion3.mappers

import dpla.ingestion3.mappers.utils.{Document, Mapping, XmlMapping}
import dpla.ingestion3.messages.{IngestMessage, IngestMessageTemplates, MessageCollector}
import dpla.ingestion3.model.{OreAggregation, _}
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.NodeSeq


class MapperTest extends FlatSpec with BeforeAndAfter with IngestMessageTemplates {

  class MapTest extends Mapper[NodeSeq, XmlMapping] {
    override def map(document: Document[NodeSeq], mapping: Mapping[NodeSeq]): OreAggregation = ???
  }

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val mapTest = new MapTest
  val dataProviders: Seq[EdmAgent] = Seq("Person A", "Person B").map(nameOnlyAgent)
  val id = "123"

  it should "add an info warning if more than two dataProvider are given and return the first value" in {
    val message = moreThanOneValueInfo(id, "dataProvider", "Person A | Person B")
    val validatedDataProvider = mapTest.validateDataProvider(dataProviders, id)

    assert(msgCollector.getAll().contains(message))
    assert(validatedDataProvider === dataProviders.head)
  }

  it should "validate edmRights by checking size <= 1 and values are mintable uris" in {
    val rightsUri = Seq("http://rights.org", "http://rights-2.org", "dub dub dub rights.com")
    val message = mintUriError(id, "edmRights", "dub dub dub rights.com")
    val rights = rightsUri.map(URI)

    val validatedEdmRights = mapTest.validateEdmRights(rights, id)

    assert(msgCollector.getAll().contains(message))
    assert(validatedEdmRights === rights.headOption)
  }

  it should "log an error when both emdRights and rights are empty" in {
    val rights = Seq()
    val edmRight = Seq()
    val message = missingRights(id)

    mapTest.validateRights(edmRight, rights, id)
    assert(msgCollector.getAll().contains(message))
  }
}
