package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}


class SiMappingTest extends AnyFlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = "si"
  val xmlString: String = new FlatFileIO().readFileAsString("/si.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))

  val extractor = new SiMapping

  it should "use the provider shortname in minting IDs " in
    assert(extractor.useProviderName)

  it should "extract the correct contributor" in {
    val expected = Seq("Contributor").map(nameOnlyAgent)
    assert(extractor.contributor(xml) === expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("Creator").map(nameOnlyAgent)
    assert(extractor.creator(xml) === expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("Note")
    assert(extractor.description(xml) === expected)
  }

  it should "extract the correct extent" in {
    val expected = Seq("3x5 feet")
    assert(extractor.extent(xml) === expected)
  }

  it should "extract the correct format" in {
    val expected = Seq("cloth")
    assert(extractor.format(xml) === expected)
  }

  it should "extract the correct identifier" in {
    val expected = Seq("1991.0076.0102")
    assert(extractor.identifier(xml) === expected)
  }

  it should "extract the correct original identifier " in
    assert(extractor.originalId(xml) === Some("http://collections.si.edu/search/results.htm?q=record_ID%%3Aacm_1991.0076.0102&repo=DPLA"))

  it should "extract the correct isShownAt value" in {
    val expected = Seq("http://collections.si.edu/search/results.htm?q=record_ID=acm_1991.0076.0102&repo=DPLA")
      .map(stringOnlyWebResource)
    assert(extractor.isShownAt(xml) === expected)
  }

  it should "build an isShownAt from the guid if it's available" in {
    val xml =
      <doc>
        <descriptiveNonRepeating>
          <guid>http://example.com/12345</guid>
        </descriptiveNonRepeating>
      </doc>

    val expected = Seq("http://example.com/12345").map(stringOnlyWebResource)
    assert(extractor.isShownAt(Document(xml)) === expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("English").map(nameOnlyConcept)
    assert(extractor.language(xml) === expected)
  }

  it should "extract the correct place when no geoLocation" in {
    val xml =
      <doc>
        <freetext>
          <place>Place</place>
        </freetext>
      </doc>
    val expected = Seq("Place").map(nameOnlyPlace)
    assert(extractor.place(Document(xml)) === expected)
  }

  it should "extract the correct place when geoLocation present" in {
    val xml =
      <doc>
        <indexedStructured>
          <geoLocation>
            <L2 type="Country">Country</L2>
          </geoLocation>
        </indexedStructured>
        <freetext>
          <place>Place</place>
        </freetext>
      </doc>
    val expected = Seq(DplaPlace(country = Some("Country")))
    assert(extractor.place(Document(xml)) === expected)
  }

  it should "extract the correct coordinates when geoLocation present" in {
    val xml =
      <doc>
        <indexedStructured>
          <geoLocation>
            <points>
              <point>
                <latitude type="decimal">100.1</latitude>
                <longitude type="decimal">24.2</longitude>
              </point>
            </points>
          </geoLocation>
        </indexedStructured>
        <freetext>
          <place>Place</place>
        </freetext>
      </doc>
    val expected = Seq(DplaPlace(coordinates = Some("100.1,24.2")))
    assert(extractor.place(Document(xml)) === expected)
  }

  it should "extract the correct preview values" in {
    val expected = Seq(
      "http://ids.si.edu/ids/deliveryService?id=ACM-acmobj-199100760102-r2",
      "http://ids.si.edu/ids/deliveryService?id=ACM-acmobj-199100760102-r1-000002",
      "http://ids.si.edu/ids/deliveryService?id=ACM-acmobj-199100760102-r3"
    ).map(URI(_))

    assert(expected.contains(extractor.preview(xml).map(_.uri).head))
  }

  it should "extract the correct publisher" in {
    val expected = Seq("publisher").map(nameOnlyAgent)
    assert(extractor.publisher(xml) === expected)
  }

  it should "extract the correct rights statement" in {
    val expected = Seq("credit line rights statement")
    assert(extractor.rights(xml) === expected)
  }

  it should "extract rights from online_media" in {
    val xml =
      <doc>
        <descriptiveNonRepeating>
          <online_media mediaCount="3">
            <media idsId="" thumbnail="" type="Images" rights="rights attr">http://ids.si.edu/ids/deliveryService?id=ACM-acmobj-199100760102-r2</media>
            <media idsId="" thumbnail="" type="Images" rights="rights attr 2">http://ids.si.edu/ids/deliveryService?id=ACM-acmobj-199100760102-r2</media>
          </online_media>
        </descriptiveNonRepeating>
      </doc>

    val expected = Seq("rights attr", "rights attr 2")
    assert(extractor.rights(Document(xml)) === expected)
  }

  it should "extract edmRights from online_media when objectRights label maps to RS.org" in {
    val xml =
      <doc>
        <descriptiveNonRepeating>
          <objectRights label="Restrictions &amp; Rights">
                No Known Copyright
              </objectRights>
        </descriptiveNonRepeating>
      </doc>

    val expected = Seq(URI("http://rightsstatements.org/vocab/NKC/1.0/"))
    assert(extractor.edmRights(Document(xml)) === expected)
  }

  it should "extract edmRights from media \\ usage \\ access when conflicting labels" in {
    val xml =
      <doc>
        <descriptiveNonRepeating>
          <media>
            <usage>
              <access>CC0</access>
            </usage>
          </media>
          <media>
            <usage>
              <access>usage conditions apply</access>
            </usage>
          </media>
        </descriptiveNonRepeating>
      </doc>

    val expected = Seq(URI("http://rightsstatements.org/vocab/InC/1.0/"))
    assert(extractor.edmRights(Document(xml)) === expected)
  }

  it should "extract edmRights from media \\ usage \\ access when cc0" in {
    val xml =
      <doc>
        <descriptiveNonRepeating>
          <media>
            <usage>
              <access>CC0</access>
            </usage>
          </media>
          <media>
            <usage>
              <access>CC0</access>
            </usage>
          </media>
        </descriptiveNonRepeating>
      </doc>

    val expected = Seq(URI("http://creativecommons.org/publicdomain/zero/1.0/"))
    assert(extractor.edmRights(Document(xml)) === expected)
  }

  it should "extract the correct subjects" in {
    val expected = Seq("topic").map(nameOnlyConcept)
    assert(extractor.subject(xml) === expected)
  }

  it should "extract the correct temporal value" in {
    val expected = Seq("19th century").map(stringOnlyTimeSpan)
    assert(extractor.temporal(xml) === expected)
  }

  it should "extract the correct titles" in {
    val expected = Seq("Dashiki with heart shaped patterns", "Object name")
    assert(extractor.title(xml) === expected)
  }

  it should "extract the correct types" in {
    val expected = Seq("dashiki", "cloth", "3x5 feet") // these will get cleaned up by type enrichment
    assert(extractor.`type`(xml) === expected)
  }

  it should "extract the correct mediaMaster" in {
    val xml = <doc>
      <descriptiveNonrepeating>
        <online_media>
          <media thumbnail="https://ids.si.edu/ids/deliveryService/id/ark:/65665/m354db31afb0f64d4c8a06b75b1179e81b/90" type="Images">
            <usage>
              <access>CC0</access>
            </usage>
              https://ids.si.edu/ids/deliveryService/id/ark:/65665/m354db31afb0f64d4c8a06b75b1179e81b
          </media>
          <media thumbnail="https://ids.si.edu/ids/deliveryService/id/ark:/65665/m3d9f23b1f4f1d4b1eb31fd6df431027aa/90" type="Images">
            <usage>
              <access>CC0</access>
            </usage>
            https://ids.si.edu/ids/deliveryService/id/ark:/65665/m3d9f23b1f4f1d4b1eb31fd6df431027aa
          </media>
        </online_media>
      </descriptiveNonrepeating>
    </doc>

    val expected = Seq(
      "https://ids.si.edu/ids/deliveryService/id/ark:/65665/m354db31afb0f64d4c8a06b75b1179e81b",
      "https://ids.si.edu/ids/deliveryService/id/ark:/65665/m3d9f23b1f4f1d4b1eb31fd6df431027aa"
    ).map(stringOnlyWebResource)

    val result = extractor.mediaMaster(Document(xml))

    assert(result === expected)
  }
}
