package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model._
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.NodeSeq


class PaMappingTest extends AnyFlatSpec with BeforeAndAfter {

  val extractor = new PaMapping

  val xml: Document[NodeSeq] = Document(<record>
  <header>
    <identifier>oai:YOUR_OAI_PREFIX:dpla_test:oai:libcollab.temple.edu:dplapa:BALDWIN_kthbs_arch_699</identifier>
    <datestamp>2019-08-29T16:57:36Z</datestamp>
    <setSpec>dpla_test</setSpec>
  </header>
  <metadata>
    <oai_dc:dc>
      <dcterms:title>Chuck-Wagon Picnic - 1955</dcterms:title>
      <dcterms:date>1955</dcterms:date>
      <dcterms:subject>Students</dcterms:subject>
      <dcterms:type>Image</dcterms:type>
      <dcterms:language>English</dcterms:language>
      <dcterms:rights>This digital object is protected under U.S. and international copyright laws and is copyright by The Baldwin School. It may not be used for any purpose without express written consent by The Baldwin School. Contact the Anne Frank Library for more information.</dcterms:rights>
      <dcterms:identifier>dplapa:BALDWIN_kthbs_arch_699</dcterms:identifier>
      <edm:isShownAt>http://digitalcollections.powerlibrary.org/cdm/ref/collection/kthbs-arch/id/699</edm:isShownAt>
      <edm:preview>http://digitalcollections.powerlibrary.org/utils/getthumbnail/collection/kthbs-arch/id/699</edm:preview>
      <dcterms:isPartOf>The Baldwin School - Photos and Documents</dcterms:isPartOf>
      <edm:dataProvider>Baldwin School Archives</edm:dataProvider>
      <dpla:intermediateProvider>POWER Library as sponsor and HSLC as maintainer</dpla:intermediateProvider>
      <edm:provider>PA Digital</edm:provider>
      <dcterms:isReferencedBy>http://digital.library.temple.edu/iiif/info/p15037coll3/71549/manifest.json</dcterms:isReferencedBy>
      <padig:mediaMaster>http://media.master/1</padig:mediaMaster>
      <padig:mediaMaster>http://media.master/2</padig:mediaMaster>
      <padig:mediaMaster>http://media.master/3</padig:mediaMaster>
    </oai_dc:dc>
  </metadata>
</record>)

  it should "extract the correct mediaMasters" in {
    val expected = Seq("http://media.master/1", "http://media.master/2", "http://media.master/3").map(stringOnlyWebResource)
    assert(extractor.mediaMaster(xml) === expected)
  }
  it should "extract the correct IIIF manifest" in {
    val expected = Seq(URI("http://digital.library.temple.edu/iiif/info/p15037coll3/71549/manifest.json"))
    assert(extractor.iiifManifest(xml) === expected)
  }
  it should "extract the correct original ID" in {
    val expected = Some("oai:YOUR_OAI_PREFIX:dpla_test:oai:libcollab.temple.edu:dplapa:BALDWIN_kthbs_arch_699")
    assert(extractor.originalId(xml) == expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("Baldwin School Archives").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) === expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("English").map(nameOnlyConcept)
    assert(extractor.language(xml) === expected)
  }
}
