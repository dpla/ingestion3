package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.IngestLogLevel
import dpla.ingestion3.model._
import dpla.ingestion3.utils.{CHProviderRegistry, FlatFileIO}
import org.apache.commons.codec.digest.DigestUtils
import org.scalatest.flatspec.AnyFlatSpec

import scala.xml.{NodeSeq, XML}

class MaineMappingTest extends AnyFlatSpec {

  private def read(name: String): String =
    new FlatFileIO().readFileAsString(s"/maine/$name")

  private def load(name: String): Document[NodeSeq] =
    Document(XML.loadString(read(name)))

  val extractor = new MaineMapping

  // 2017 legacy record, statedocs, has dc:contributor
  val statedocs: Document[NodeSeq] = load("maine-statedocs.xml")
  // 2017 legacy record whose Digital Commons record has no dc:contributor
  val noContributor: Document[NodeSeq] = load("maine-no-contributor.xml")
  // 2017 legacy record harvested through the REPOX msl_regions set
  val regions: Document[NodeSeq] = load("maine-regions.xml")
  // Record added to Digital Commons after the 2017 harvest
  val added: Document[NodeSeq] = load("maine-new.xml")

  // ID continuity

  "MaineMapping" should "use the REPOX-wrapped originalId for legacy statedocs records" in {
    assert(
      extractor.originalId(statedocs) === Some(
        "oai:the.european.library.msl_statedocs:oai:digitalmaine.com:alien_docs-5510"
      )
    )
  }

  it should "keep the existing DPLA id for a legacy statedocs record" in {
    assert(
      extractor.dplaUri(statedocs) === Some(
        URI("http://dp.la/api/items/bb60846e422391626e81900b7773eff0")
      )
    )
  }

  it should "keep the existing DPLA id for a legacy record without a contributor" in {
    assert(
      extractor.dplaUri(noContributor) === Some(
        URI("http://dp.la/api/items/b38cf47a4dad65d9069bbb580b810c0e")
      )
    )
  }

  it should "keep the existing DPLA id for a legacy regions record" in {
    assert(
      extractor.originalId(regions) === Some(
        "oai:the.european.library.msl_regions:oai:digitalmaine.com:brewer_images-1010"
      )
    )
    assert(
      extractor.dplaUri(regions) === Some(
        URI("http://dp.la/api/items/a23effa7a89f84c0612f63672040d832")
      )
    )
  }

  it should "use the Digital Commons identifier as-is for records added since 2017" in {
    assert(
      extractor.originalId(added) === Some(
        "oai:digitalmaine.com:trolley_images-1001"
      )
    )
    assert(
      extractor.dplaUri(added) === Some(
        URI("http://dp.la/api/items/f9fe6bbb14575bf7e2da0e9770c18685")
      )
    )
  }

  it should "resolve identifiers that were in both REPOX sets to msl_statedocs" in {
    assert(
      MaineMapping.legacyOriginalId(
        "oai:digitalmaine.com:arc_postcard-1013"
      ) ===
        "oai:the.european.library.msl_statedocs:oai:digitalmaine.com:arc_postcard-1013"
    )
  }

  it should "reproduce the recorded DPLA id for every crosswalk entry" in {
    val rows = new FlatFileIO()
      .readFileAsSeq("/maine/legacy_ids.tsv")
      .drop(1)
      .map(_.split("\t"))
    assert(rows.size === 63448)
    val mismatches = rows.filterNot { case Array(id, _, dplaId) =>
      DigestUtils.md5Hex(
        "maine--" + MaineMapping.legacyOriginalId(id)
      ) == dplaId
    }
    assert(mismatches.isEmpty, mismatches.take(5).map(_.mkString("\t")))
  }

  // Full mapping through the registered profile

  private def errors(name: String) =
    CHProviderRegistry
      .lookupProfile("maine")
      .get
      .mapOreAggregation(read(name))
      .messages
      .filter(_.level == IngestLogLevel.error)

  it should "map a complete record through the maine profile without errors" in {
    assert(errors("maine-statedocs.xml").isEmpty)
  }

  it should "fail a record that has no dc:contributor (dataProvider is required)" in {
    assert(
      errors("maine-no-contributor.xml").map(m => (m.field, m.message)) ===
        Seq(("dataProvider", "Missing required field"))
    )
  }

  // Aggregation fields

  it should "extract the dataProvider from dc:contributor" in {
    assert(
      extractor.dataProvider(statedocs) === Seq(
        nameOnlyAgent("Maine State Archives")
      )
    )
  }

  it should "map no dataProvider when dc:contributor is absent" in {
    assert(extractor.dataProvider(noContributor) === Seq())
  }

  it should "extract isShownAt from dc:identifier" in {
    assert(
      extractor.isShownAt(statedocs) === Seq(
        stringOnlyWebResource("https://digitalmaine.com/alien_docs/4510")
      )
    )
  }

  it should "extract the thumbnail URL from dc:description as preview" in {
    assert(
      extractor.preview(statedocs) === Seq(
        stringOnlyWebResource(
          "https://digitalmaine.com/alien_docs/5510/thumbnail.jpg"
        )
      )
    )
  }

  it should "extract edmRights from dc:rights URIs" in {
    assert(
      extractor.edmRights(statedocs) === Seq(
        URI("http://rightsstatements.org/vocab/NoC-US/1.0/")
      )
    )
    assert(extractor.rights(statedocs) === Seq())
  }

  it should "keep non-rights-statement URLs in dc:rights as free-text rights" in {
    val rec: Document[NodeSeq] = Document(
      XML.loadString(
        read("maine-regions.xml").replace(
          "<dc:rights>http://rightsstatements.org/vocab/NoC-US/1.0/</dc:rights>",
          "<dc:rights>http://rightsstatements.org/vocab/NoC-US/1.0/</dc:rights>" +
            "<dc:rights>https://digitalmaine.com/rights.html</dc:rights>"
        )
      )
    )
    assert(
      extractor.edmRights(rec) === Seq(
        URI("http://rightsstatements.org/vocab/NoC-US/1.0/")
      )
    )
    assert(
      extractor.rights(rec) === Seq("https://digitalmaine.com/rights.html")
    )
  }

  it should "use the correct provider" in {
    assert(
      extractor.provider(statedocs) === EdmAgent(
        name = Some("Digital Maine"),
        uri = Some(URI("http://dp.la/api/contributor/maine"))
      )
    )
  }

  // SourceResource fields

  it should "take the collection from the record's own publication set name" in {
    assert(
      extractor.collection(statedocs) === Seq(
        nameOnlyCollection("Registrations")
      )
    )
    assert(
      extractor.collection(regions) === Seq(nameOnlyCollection("Brewer Images"))
    )
    assert(
      extractor.collection(added) === Seq(
        nameOnlyCollection(
          "New England Electric Railway Historical Society Images"
        )
      )
    )
  }

  it should "map no collection when the harvest recorded no set names" in {
    assert(extractor.collection(noContributor) === Seq())
  }

  it should "extract the title" in {
    assert(
      extractor.title(regions) === Seq("Sargent Lumber Mill, Brewer, Maine")
    )
  }

  it should "extract the description from dc:description.abstract, not the thumbnail" in {
    assert(extractor.description(regions) === Seq("Sargent Lumber Mill"))
  }

  it should "extract the creator" in {
    assert(
      extractor.creator(regions) === Seq(
        nameOnlyAgent("J.Craig Thayer Photography")
      )
    )
  }

  it should "extract the date from dc:date.created as given" in {
    assert(
      extractor.date(added) === Seq(stringOnlyTimeSpan("1906-01-01T08:00:00Z"))
    )
  }

  it should "extract the type" in {
    assert(extractor.`type`(regions) === Seq("Image"))
  }

  it should "split semicolon-delimited subjects" in {
    assert(
      extractor.subject(noContributor).map(_.providedLabel.get) === Seq(
        "Library",
        "Public Library Buildings",
        "Maine",
        "Winterport",
        "Maine"
      )
    )
  }

  it should "extract place names and a coordinates-only place" in {
    assert(
      extractor.place(noContributor) === Seq(
        nameOnlyPlace("Winterport"),
        nameOnlyPlace("Waldo County"),
        DplaPlace(coordinates = Some("44.6352984, -68.84673750000002"))
      )
    )
  }

  it should "extract place names without coordinates when lat/long are absent" in {
    assert(
      extractor.place(statedocs) === Seq(
        nameOnlyPlace("Biddeford"),
        nameOnlyPlace("York County")
      )
    )
  }

  it should "extract the language" in {
    assert(
      extractor.language(statedocs) === Seq(nameOnlyConcept("English"))
    )
  }
}
