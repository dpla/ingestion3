package dpla.ingestion3.harvesters.file

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

/** Unit tests for the NGA CSV join/assembly logic. These exercise
  * [[NgaFileHarvester]]'s pure helpers directly (no Spark) against the small
  * fixture set in `src/test/resources/nga/`.
  */
class NgaFileHarvesterTest
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private val dataDir =
    new File(getClass.getResource("/nga/objects.csv").toURI).getParentFile

  private def rows(name: String) =
    NgaFileHarvester.readRows(new File(dataDir, name))

  private def in(name: String) = new File(dataDir, name)

  // Build the same indexes harvest() builds, from the fixtures.
  private lazy val imagesByObj =
    NgaFileHarvester.indexBy(in("published_images.csv"), "depictstmsobjectid")
  private lazy val constLinksByObj =
    NgaFileHarvester.indexBy(in("objects_constituents.csv"), "objectid")
  private lazy val constById =
    NgaFileHarvester.indexUniqueBy(in("constituents.csv"), "constituentid")
  private lazy val termsByObj =
    NgaFileHarvester.indexBy(in("objects_terms.csv"), "objectid")
  private lazy val textsByObj =
    NgaFileHarvester.indexBy(in("objects_text_entries.csv"), "objectid")

  private def docFor(objectId: String): JValue = {
    val obj = rows("objects.csv").find(_("objectid") == objectId).get
    NgaFileHarvester.buildDocument(
      obj,
      imagesByObj,
      constLinksByObj,
      constById,
      termsByObj,
      textsByObj
    )
  }

  private implicit val formats: Formats = DefaultFormats

  "readRows" should "parse quoted fields with embedded newlines" in {
    val obj0 = rows("objects.csv").find(_("objectid") == "0").get
    obj0("medium") should include("\n")
    obj0("title") shouldBe "Saint James Major"
  }

  it should "tolerate ragged rows (short/long) without aborting" in {
    // NGA's real export has occasional rows with a field count != the header.
    val r = rows("ragged.csv")
    r should have size 3
    r.head("role") shouldBe "artist"
    r(1).get("role") shouldBe None // short row: trailing columns simply absent
    r(1)("constituentid") shouldBe "11"
    r(2)("role") shouldBe "donor" // long row: extra value beyond the header dropped
  }

  "buildDocument" should "carry the object's own columns at the top level" in {
    val doc = docFor("0")
    (doc \ "title").extract[String] shouldBe "Saint James Major"
    (doc \ "accessionnum").extract[String] shouldBe "1937.1.2.c"
    (doc \ "wikidataid").extract[String] shouldBe "Q123"
  }

  it should "join published_images via depictstmsobjectid" in {
    val images = (docFor("0") \ "images").children
    images should have size 1
    (images.head \ "iiifthumburl")
      .extract[String] should endWith("!200,200/0/default.jpg")
    (images.head \ "openaccess").extract[String] shouldBe "1"
    (images.head \ "viewtype").extract[String] shouldBe "primary"
  }

  it should "join constituents and enrich each link with its constituent record" in {
    val constituents = (docFor("0") \ "constituents").children
    constituents should have size 2 // one artist, one donor

    val artist = constituents
      .find(c => (c \ "roletype").extract[String] == "artist")
      .get
    (artist \ "role").extract[String] shouldBe "painter"
    (artist \ "constituent" \ "forwarddisplayname")
      .extract[String] shouldBe "Ambrogio Lorenzetti"
    (artist \ "constituent" \ "ulanid").extract[String] shouldBe "500115983"
  }

  it should "join terms and text entries" in {
    val doc = docFor("0")
    (doc \ "terms").children should have size 2
    val texts = (doc \ "textEntries").children
    texts.map(t => (t \ "texttype").extract[String]) should contain allOf
      ("brief_narrative", "bibliography")
  }

  it should "produce empty child arrays for an object with no images/terms/text" in {
    val doc = docFor("999")
    (doc \ "images").children shouldBe empty
    (doc \ "terms").children shouldBe empty
    (doc \ "textEntries").children shouldBe empty
    // still has its single (anonymous) artist link
    val constituents = (doc \ "constituents").children
    constituents should have size 1
    (constituents.head \ "constituent" \ "preferreddisplayname")
      .extract[String] shouldBe "Anonymous"
  }

  it should "render as valid, parseable JSON" in {
    val json = compact(render(docFor("0")))
    parse(json) // throws if not valid JSON
    json should include("\"objectid\":\"0\"")
  }
}
