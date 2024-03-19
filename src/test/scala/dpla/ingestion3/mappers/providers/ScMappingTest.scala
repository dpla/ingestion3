package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

class ScMappingTest extends AnyFlatSpec with BeforeAndAfter {

  val shortName = "scdl"
  val xmlString: String = new FlatFileIO().readFileAsString("/scdl.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val extractor = new ScMapping

  it should "extract the correct originalId" in {
    val expected = Some("oai:scmemory-search.org/oai-tigerprints-clemson-edu-spec_agrarian-1006")
    assert(extractor.originalId(xml) == expected)
  }

  it should "extract the correct dataProvider" in {
    val expected = Seq("Clemson University Libraries").map(nameOnlyAgent)
    assert(extractor.dataProvider(xml) == expected)
  }

  it should "map accessRight to rights " in {
    val xml =
      <record>
        <metadata>
          <oai_qdc:qualifieddc>
            <dcterms:accessRights>http://rightsstatements.org/vocab/InC/1.0/</dcterms:accessRights>
          </oai_qdc:qualifieddc>
        </metadata>
      </record>

    val expected = Seq("http://rightsstatements.org/vocab/InC/1.0/")
    assert(expected === extractor.rights(Document(xml)))
  }

  it should "extract the correct isShownAt" in {
    val expected = Seq("http://tigerprints.clemson.edu/spec_agrarian/7").map(stringOnlyWebResource)
    assert(extractor.isShownAt(xml) == expected)
  }

  it should "extract the correct contributor" in {
    val expected = Seq("Name of contributor").map(nameOnlyAgent)
    assert(extractor.contributor(xml) == expected)
  }

  it should "extract the correct creator" in {
    val expected = Seq("University, Clemson").map(nameOnlyAgent)
    assert(extractor.creator(xml) == expected)
  }

  it should "extract the correct date" in {
    val expected = Seq("1940-12-01T08:00:00Z").map(stringOnlyTimeSpan)
    assert(extractor.date(xml) == expected)
  }

  it should "extract the correct description" in {
    val expected = Seq("http://tigerprints.clemson.edu/spec_agrarian/1006/thumbnail.jpg")
    assert(extractor.description(xml) == expected)
  }

  it should "extract the correct extent" in {
    val expected = Seq("Extent")
    assert(extractor.extent(xml) == expected)
  }

  it should "extract the correct format" in {
    val expected = Seq("Periodicals")
    assert(extractor.format(xml) == expected)
  }

  it should "extract the correct identifier" in {
    val expected = Seq("http://tigerprints.clemson.edu/spec_agrarian/7")
    assert(extractor.identifier(xml) == expected)
  }

  it should "extract the correct language" in {
    val expected = Seq("English").map(nameOnlyConcept)
    assert(extractor.language(xml) == expected)
  }

  it should "extract the correct place" in {
    val expected = Seq("Pickens County (S.C.)").map(nameOnlyPlace)
    assert(extractor.place(xml) == expected)
  }

  it should "extract the correct rights" in {
    val expected = Seq("Copyright of Clemson University. Use of materials from this collection beyond the exceptions provided for in the Fair Use and Educational Use clauses of the U.S. Copyright Law may violate federal law. Permission to publish or reproduce is required.")
    assert(extractor.rights(xml) == expected)
  }

  it should "extract the correct subject" in {
    val expected = Seq("Agriculture").map(nameOnlyConcept)
    assert(extractor.subject(xml) == expected)
  }

  it should "extract the correct temporal" in {
    val expected = Seq("Temporal").map(stringOnlyTimeSpan)
    assert(extractor.temporal(xml) == expected)
  }

  it should "extract the correct title" in {
    val expected = Seq("The Agrarian Vol. 3 No. 1")
    assert(extractor.title(xml) == expected)
  }

  it should "extract the correct type" in {
    val expected = Seq("Book", "Text")
    assert(extractor.`type`(xml) == expected)
  }

  it should "extract the correct preview" in {
    val expected = Seq("http://tigerprints.clemson.edu/spec_agrarian/1006/thumbnail.jpg")
      .map(stringOnlyWebResource)
    assert(extractor.preview(xml) == expected)
  }
}
