package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.{Document, MappingException}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

class Jh3MappingTest extends AnyFlatSpec with BeforeAndAfter {

  val shortName = "jh3"
  val xmlString: String = new FlatFileIO().readFileAsString("/jh3.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new Jh3Mapping

  it should "extract the correct originalId" in {
    val expected = Some("blavatnik_postcards:10131")
    assert(extractor.originalId(xml) == expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("The Blavatnik Archive").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) == expected)
  }
    it should "extract the correct edmRights" in {
      val expected = Seq("http://rightsstatements.org/vocab/InC/1.0/").map(URI)
      assert(extractor.edmRights(xml) == expected)
    }

    it should "extract the correct isShownAt" in {
      val expected = Seq("http://n2t.net/ark:/86084/b4p490")
        .map(stringOnlyWebResource)
      assert(extractor.isShownAt(xml) == expected)
    }
  //
  //  it should "extract the correct alternate title" in {
  //    val expected = Seq("Heuchera villosa Michx.")
  //    assert(extractor.alternateTitle(xml) == expected)
  //  }

//    it should "extract the correct contributor" in {
//      val expected = Seq("Name of contributor").map(nameOnlyAgent)
//      assert(extractor.contributor(xml) == expected)
//    }

  //  it should "extract the correct creator" in {
  //    val expected = Seq("Ray C. Friesner", "Sallie", "Ron", "Sterling").map(nameOnlyAgent)
  //    assert(extractor.creator(xml) == expected)
  //  }

    it should "extract the correct date" in {
      val expected = Seq("1941 to 1942").map(stringOnlyTimeSpan)
      assert(extractor.date(xml) == expected)
    }

    it should "extract the correct description" in {
      val expected = Seq("description")
      assert(extractor.description(xml) == expected)
    }

    it should "extract the correct format" in {
      val expected = Seq("Ephemera", "Ephemera")
      assert(extractor.format(xml) == expected)
    }

    it should "extract the correct identifier" in {
      val expected = Seq("identifier")
      assert(extractor.identifier(xml) == expected)
    }

    it should "extract the correct language" in {
      val expected = Seq("English").map(nameOnlyConcept)
      assert(extractor.language(xml) == expected)
    }

    it should "extract the correct place" in {
      val expected = Seq(
        DplaPlace(
          name = Some("prefLabel"),
          coordinates = Some("123.456N,123.456W")
        )
      )
      assert(extractor.place(xml) == expected)
    }

          <skos:Concept rdf:about="https://api.blavatnikarchive.org/api/services/app/blavatnik_postcards:10131#subject_1">
                <skos:prefLabel>New Year</skos:prefLabel>
                <skos:altLabel>Holidays, festivals, celebrations >> New Year</skos:altLabel>
                <skos:note>New Year, 1941 - 1942</skos:note>
            </skos:Concept>
            <skos:Concept rdf:about="https://api.blavatnikarchive.org/api/services/app/blavatnik_postcards:10131#subject_2">
                <skos:prefLabel>Siege of Leningrad, 1941-1944</skos:prefLabel>
                <skos:altLabel>
                    World War II, battles and campaigns >> Siege of Leningrad, 1941-1944
                </skos:altLabel>
                <skos:note>Siege of Leningrad, 1941-1944, 1941 - 1942</skos:note>
            </skos:Concept>

  // Helper: minimal JH3 record wrapper with Mojibake in a title element
  private def mojibakeDoc(titleText: String): Document[NodeSeq] = Document(
    XML.loadString(
      s"""<record xmlns="http://www.openarchives.org/OAI/2.0/">
         |  <header><identifier>test:1</identifier></header>
         |  <metadata>
         |    <rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
         |             xmlns:dc="http://purl.org/dc/elements/1.1/"
         |             xmlns:edm="http://www.europeana.eu/schemas/edm/"
         |             xmlns:ore="http://www.openarchives.org/ore/terms/">
         |      <edm:ProvidedCHO rdf:about="test:1">
         |        <dc:title>$titleText</dc:title>
         |      </edm:ProvidedCHO>
         |      <ore:Aggregation rdf:about="test:1#agg">
         |        <edm:dataProvider>Test</edm:dataProvider>
         |        <edm:isShownAt rdf:resource="http://example.org/1"/>
         |        <edm:provider>Jewish Heritage Network</edm:provider>
         |        <edm:rights rdf:resource="http://rightsstatements.org/vocab/InC/1.0/"/>
         |      </ore:Aggregation>
         |    </rdf:RDF>
         |  </metadata>
         |</record>""".stripMargin
    )
  )

  it should "throw MappingException when Hebrew Mojibake is present" in {
    // U+00D7 U+00A9 = UTF-8 bytes 0xD7 0xA9 (Hebrew shin ש) misread as Latin-1
    val doc = mojibakeDoc("\u00d7\u00a9\u00d7\u00aa\u00d7\u0099")
    assertThrows[MappingException] { extractor.rights(doc) }
  }

  it should "throw MappingException when Cyrillic Mojibake is present" in {
    // U+00D0 U+00BF = UTF-8 bytes 0xD0 0xBF (Cyrillic п) misread as Latin-1
    val doc = mojibakeDoc("\u00d0\u00bf\u00d1\u0080\u00d0\u00b8\u00d0\u00b2\u00d0\u00b5\u00d1\u0082")
    assertThrows[MappingException] { extractor.rights(doc) }
  }

  it should "not throw on clean Hebrew text" in {
    // Proper UTF-8 Hebrew: שלום
    val doc = mojibakeDoc("\u05e9\u05dc\u05d5\u05dd")
    assert(extractor.rights(doc) == Seq())
  }

  it should "not throw on clean Cyrillic text" in {
    // Proper UTF-8 Cyrillic: привет
    val doc = mojibakeDoc("\u043f\u0440\u0438\u0432\u0435\u0442")
    assert(extractor.rights(doc) == Seq())
  }

  it should "extract the correct subject from skos:Concept" in {
      val expected = Seq(
        SkosConcept(
          providedLabel = Some("New Year"),
          note = Some("New Year, 1941 - 1942"),
          exactMatch = Seq(URI("https://api.blavatnikarchive.org/api/services/app/blavatnik_postcards:10131#subject_1"))
        ),
        SkosConcept(
          providedLabel = Some("Siege of Leningrad, 1941-1944"),
          note = Some("Siege of Leningrad, 1941-1944, 1941 - 1942"),
          exactMatch = Seq(URI("https://api.blavatnikarchive.org/api/services/app/blavatnik_postcards:10131#subject_2"))
        )
      )
      assert(extractor.subject(xml) == expected)
    }

}
