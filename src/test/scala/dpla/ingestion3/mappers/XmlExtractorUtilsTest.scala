package dpla.ingestion3.mappers.xml

import dpla.ingestion3.mappers.utils.XmlExtractor
import org.scalatest._

import scala.xml.NodeSeq

class XmlExtractorUtilsTest extends FlatSpec with XmlExtractor {

  it should "return a None when there is no field named by a string" in {
    val xml: NodeSeq = <ore:Aggregation rdf:about="http://harvester.orbiscascade.org/record/e466e93cf4849fd8aa36f1daa1417c15"
                               xmlns:dcterms="http://www.w3.org/1999/xhtml"
                               xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns"
                               xmlns:doap="http://usefulinc.com/ns/doap"
                               xmlns:edm="http://rs.org/">
      </ore:Aggregation>

    println("metadata " + xml.head.getNamespace("rdf"))
    println("metadata " + xml.head.getNamespace("edm"))

    val rdfNs = xml \ "@{xmlns}rdf}"
    println(rdfNs)
    val attr = xml \ "@{http://www.w3.org/1999/02/22-rdf-syntax-ns}about"
    println(xml)
    println("attr " + attr)

    // assert(getAttributeValue("about")(xml) === "http://harvester.orbiscascade.org/record/e466e93cf4849fd8aa36f1daa1417c15")
  }

}

