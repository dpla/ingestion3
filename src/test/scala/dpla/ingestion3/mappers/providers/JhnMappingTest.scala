package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

class JhnMappingTest extends AnyFlatSpec with BeforeAndAfter {

  val shortName = "jhn"
  val xmlString: String = new FlatFileIO().readFileAsString("/jhn.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new JhnMapping

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
