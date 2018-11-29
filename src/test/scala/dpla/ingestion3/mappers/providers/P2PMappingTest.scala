package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.NodeSeq

class P2PMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]

  val mapping = new P2PMapping

  "A P2PMapping" should "have the correct provider name" in {
    assert(mapping.getProviderName === "p2p")
  }

  it should "get the correct provider ID" in {
    implicit val data = Document(
      <record>
        <header>
          <identifier>oai:plains2peaks:Pine_River_2019-01:oai:prlibrary.cvlcollections.org:54</identifier>
        </header>
      </record>.asInstanceOf[NodeSeq]
    )
    assert(mapping.getProviderId === "oai:plains2peaks:Pine_River_2019-01:oai:prlibrary.cvlcollections.org:54")
  }

  //TODO
  it should "mint the correct DPLA URI" in {
    implicit val data = Document(
      <record></record>
    )
  }

  it should "return the correct data provider" in {
    implicit val data = Document(
      <record>
        <metadata>
          <mods>
            <note type="ownership">Foo</note>
          </mods>
        </metadata>
      </record>.asInstanceOf[NodeSeq]
    )

    val result = mapping.dataProvider(data)
    assert(result.head.name === Some("Foo"))

  }


}