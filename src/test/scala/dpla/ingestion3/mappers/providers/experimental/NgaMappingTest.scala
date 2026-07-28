package dpla.ingestion3.mappers.providers.experimental

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.flatspec.AnyFlatSpec

/** First-pass tests for the NGA mapping, against a real assembled harvester
  * record (object 55035 — an engraving with two artists, one open-access image,
  * ULAN + Wikidata authority IDs, and keyword terms).
  */
class NgaMappingTest extends AnyFlatSpec {

  implicit val msgCollector: MessageCollector[IngestMessage] =
    new MessageCollector[IngestMessage]

  private def load(name: String): Document[JValue] =
    Document(parse(new FlatFileIO().readFileAsString(name)))

  // 55035: engraving, one OPEN-ACCESS image, two artists.
  private val doc: Document[JValue] = load("/nga-object-55035.json")
  // 88206: Robert Frank photograph, image is NOT open access (openaccess=0).
  private val restricted: Document[JValue] = load("/nga-object-88206.json")
  private val m = new NgaMapping

  it should "mint IDs from the provider name" in {
    assert(m.useProviderName)
    assert(m.getProviderName.contains("nga"))
  }

  it should "use the objectid as the original id" in {
    assert(m.originalId(doc).contains("55035"))
  }

  it should "map the title" in {
    assert(m.title(doc) == Seq("Nicolas Le Camus"))
  }

  it should "map creators (artists only) with ULAN/Wikidata exactMatch" in {
    val creators = m.creator(doc)
    val names = creators.flatMap(_.name)
    assert(names == Seq("Peter Ludwig van Schuppen", "Pieter van Mol"))
    val schuppen = creators.find(_.name.contains("Peter Ludwig van Schuppen")).get
    assert(schuppen.exactMatch.map(_.value) ==
      Seq("http://vocab.getty.edu/ulan/500032370", "http://www.wikidata.org/entity/Q3388039"))
    val mol = creators.find(_.name.contains("Pieter van Mol")).get
    // van Mol has no ULAN, only Wikidata
    assert(mol.exactMatch.map(_.value) == Seq("http://www.wikidata.org/entity/Q1032847"))
  }

  it should "not map donors/owners as creators" in {
    assert(!m.creator(doc).flatMap(_.name).exists(_.contains("Mellon")))
  }

  it should "map date with begin/end/displayDate" in {
    assert(m.date(doc) == Seq(
      EdmTimeSpan(begin = Some("1678"), end = Some("1678"), originalSourceDate = Some("1678"))
    ))
  }

  it should "map Keyword/School/Technique/Theme/Style terms to subjects" in {
    // School "Flemish" now included alongside the keywords
    assert(m.subject(doc) ==
      Seq("Flemish", "Costume", "Portrait", "Man", "Frame").map(nameOnlyConcept))
  }

  it should "map description from the image visual description (assistivetext)" in {
    assert(m.description(doc).exists(_.contains("greyhounds")))
  }

  it should "map format from classification + medium" in {
    assert(m.format(doc) == Seq("Print", "engraving"))
  }

  it should "map the accession number as identifier" in {
    assert(m.identifier(doc) == Seq("1974.116.33"))
  }

  it should "map dataProvider and provider to NGA" in {
    assert(m.dataProvider(doc) == Seq(nameOnlyAgent("National Gallery of Art")))
    assert(m.provider(doc).uri.map(_.value).contains("http://dp.la/api/contributor/nga"))
  }

  it should "construct isShownAt from the objectid (legacy redirecting URL)" in {
    assert(m.isShownAt(doc).map(_.uri.value) ==
      Seq("https://www.nga.gov/collection/art-object-page.55035.html"))
  }

  it should "map preview to the primary image IIIF thumbnail" in {
    assert(m.preview(doc).map(_.uri.value) == Seq(
      "https://api.nga.gov/iiif/00b0b172-e4e6-4c4a-a4fe-d493507b7ac3/full/!200,200/0/default.jpg"
    ))
  }

  it should "map mediaMaster to full IIIF images for open-access items" in {
    assert(m.mediaMaster(doc).map(_.uri.value) == Seq(
      "https://api.nga.gov/iiif/00b0b172-e4e6-4c4a-a4fe-d493507b7ac3/full/full/0/default.jpg"
    ))
  }

  it should "set edmRights to CC0 for open-access images" in {
    assert(m.edmRights(doc).map(_.value) ==
      Seq("https://creativecommons.org/publicdomain/zero/1.0/"))
  }

  it should "default DCMI type to image" in {
    assert(m.`type`(doc) == Seq("image"))
  }

  it should "satisfy rights via the CC0 edmRights URI, with no free-text rights" in {
    // edmRights alone satisfies DPLA's rights requirement (and is preferred);
    // NGA has no free-text rights, so `rights` stays empty.
    assert(m.edmRights(doc).map(_.value) ==
      Seq("https://creativecommons.org/publicdomain/zero/1.0/"))
    assert(m.rights(doc).isEmpty)
  }

  // --- non-open-access record (88206): no rights signal → dropped by design ---

  it should "emit NEITHER rights nor edmRights for a non-open-access record" in {
    // openaccess=0 → both empty, so validateRights rejects the record (intended:
    // NGA is scoped to its open-access works, no fabricated rights).
    assert(m.rights(restricted).isEmpty)
    assert(m.edmRights(restricted).isEmpty)
  }

  it should "emit mediaMaster for every image regardless of open-access" in {
    val mm = m.mediaMaster(restricted).map(_.uri.value)
    assert(mm.size == 1)
    assert(mm.head.endsWith("/full/full/0/default.jpg"))
  }

  it should "map description from the contact-sheet visual description" in {
    assert(m.description(restricted)
      .exists(_.contains("contact sheet displaying several black and white photographs")))
  }

  it should "still map subjects from School/Technique/Theme terms" in {
    assert(m.subject(restricted) ==
      Seq("American", "gelatin silver print", "interior").map(nameOnlyConcept))
  }

  it should "map Photograph classification to DCMI type image" in {
    assert(m.`type`(restricted) == Seq("image"))
  }
}
