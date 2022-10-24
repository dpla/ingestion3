
package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model.{DcmiTypeCollection, _}
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfter, FlatSpec}

class MdlMappingTest extends FlatSpec with BeforeAndAfter {

  val jsonString: String = new FlatFileIO().readFileAsString("/mdl.json")
  val json: Document[JValue] = Document(parse(jsonString))
  val extractor = new MdlMapping

  it should "use the provider short name when minting DPLA ids" in {
    assert(extractor.useProviderName === true)
  }

  it should "return the correct provider name" in {
    assert(extractor.getProviderName === Some("minnesota"))
  }

  it should "extract the correct original ID" in {
    val expected = Some("https://archive.mpr.org/stories/2014/08/14/hmong-vendors-learn-the-law-on-legal-drug")
    assert(extractor.originalId(json) === expected)
  }

  // dataProvider

  // dplaUri
  it should "extract correct edmRights" in {
    val expected = Seq(URI("http://rightsstatements.org/public-domain"))
    assert(extractor.edmRights(json) === expected)
  }

  // intermediateProvider
  it should "extract the correct intermediateProvider" in {
    val expected = Some(nameOnlyAgent("one intermediate provider"))
    assert(extractor.intermediateProvider(json) === expected)
  }
  // TODO what if multiple intermediateProviders given?

  // isShownAt
  it should "extract the correct isShownAt" in {
    val expected = List(stringOnlyWebResource("https://archive.mpr.org/stories/2014/08/14/hmong-vendors-learn-the-law-on-legal-drug"))
    assert(extractor.isShownAt(json) === expected)
  }
  // preview
  it should "extract the correct preview" in {
    val expected = List(stringOnlyWebResource("http://image.prview.org/thumb.jpg"))
    assert(extractor.preview(json) === expected)
  }

  // provider

  // collection
  it should "extract the correct collections" in {
    val expected = List(
      DcmiTypeCollection(title = Some("collection1"), description = Some("description1"), isShownAt = None),
      DcmiTypeCollection(title = Some("collection2"), description = None, isShownAt = None)
    )
    assert(extractor.collection(json) === expected)
  }
  // contributor
  it should "extract the correct contributors" in {
    val expected = List("contrib1", "contrib2").map(nameOnlyAgent)
    assert(extractor.contributor(json) === expected)
  }
  // creator
  it should "extract the correct creator" in {
    val expected = List("creator1", "creator2").map(nameOnlyAgent)
    assert(extractor.creator(json) === expected)
  }
  // date
  it should "extract the correct date with display, begin and end" in {
    val expected = List(EdmTimeSpan(originalSourceDate = Some("2014-08-14"), begin = Some("2014"), end = Some("2014")))
    assert(extractor.date(json) === expected)
  }
  // description
  it should "extract the correct description" in {
    val expected = List("description")
    assert(extractor.description(json) === expected)
  }
  // extent
  it should "extract the correct extent" in {
    val expected = List("extent1", "extent2")
    assert(extractor.extent(json) === expected)
  }
  // format with value filter
  it should "extract the correct format" in {
    val expected = List("pencil on paper")
    assert(extractor.format(json) === expected)
  }
  // genre
  it should "extract the correct genre from type field" in {
    val expected = List("image").map(nameOnlyConcept)
    assert(extractor.genre(json) === expected)
  }
  // language
  it should "extract the correct language values" in {
    val expected = List("eng").map(nameOnlyConcept)
    assert(extractor.language(json) === expected)
  }
  // place | spatial
  it should "extract the correct place and spatial values" in {
    val expected = List(
      DplaPlace(
        name = Some("cincinnati"),
        coordinates = Some("1212.5151x1235.548")
      ),
      DplaPlace(
        name = Some("hamilton county"),
        coordinates = Some("1.34 83.32")
      )
    )
    assert(extractor.place(json) === expected)
  }
  // publisher
  it should "extract the correct publisher" in {
    val expected = List("pub1").map(nameOnlyAgent)
    assert(extractor.publisher(json) === expected)
  }
  // rights
  it should "extract the correct rights" in {
    val jsonString =
      """
      {
        "attributes": {
          "metadata": {
            "rights": "free text rights statement"
          }
        }
      }
    """"
    val json: Document[JValue] = Document(parse(jsonString))

    val expected = List("free text rights statement")
    assert(extractor.rights(json) === expected)
  }

  it should "not map non-rs or non-cc host domains to edmRights" in {
    val jsonString =
      """
      {
        "attributes": {
          "metadata": {
            "rights": "http://mhs.org/copyright"
          }
        }
      }
    """".stripMargin
    val json: Document[JValue] = Document(parse(jsonString))


    val expected = Seq()
    assert(extractor.edmRights(json) === expected)
  }

  it should "not map non-URIs to edmRights" in {
    val jsonString =
      """
      {
        "attributes": {
          "metadata": {
            "rights": "free text! free text! free text!"
          }
        }
      }
    """".stripMargin
    val json: Document[JValue] = Document(parse(jsonString))

    val expected = Seq()
    assert(extractor.edmRights(json) === expected)
  }

  it should "work when rights is empty string" in {
    val jsonString =
      """
      {
        "attributes": {
          "metadata": {
            "rights": ""
          }
        }
      }
    """".stripMargin
    val json: Document[JValue] = Document(parse(jsonString))

    val expected = Seq()
    assert(extractor.edmRights(json) === expected)
  }

  it should "work when rights not provided" in {
    val jsonString =
      """
      {
        "attributes": {
          "metadata": {
          }
        }
      }
    """".stripMargin
    val json: Document[JValue] = Document(parse(jsonString))

    val expected = Seq()
    assert(extractor.edmRights(json) === expected)
  }

  // rights
  it should "map to dcRights but not edmRights when rights value contains both text and uri" in {
    val jsonString =
      """
      {
        "attributes": {
          "metadata": {
            "rights": "text of cc statement; http://creativecommons.org/"
          }
        }
      }
    """".stripMargin
    val json: Document[JValue] = Document(parse(jsonString))

    val expectedEmpty = List()
    val expectedRights = List("text of cc statement; http://creativecommons.org/")
    assert(extractor.edmRights(json) === expectedEmpty)
    assert(extractor.rights(json) === expectedRights)
  }

  // rights
  it should "not map rs.org value to dc rights" in {
    val expected = List()
    assert(extractor.rights(json) === expected)
  }

  // subject
  it should "extract the correct subjects" in {
    val expected = List("MPR News Feature", "Reports", "Cultural Legacy Digitization (2016-2017)")
      .map(nameOnlyConcept)
    assert(extractor.subject(json) === expected)
  }
  // title
  it should "extract the correct title" in {
    val expected = List("Hmong vendors learn the law on legal drug sales")
    assert(extractor.title(json) === expected)
  }
  // type
  it should "extract the correct type" in {
    val expected = List("image")
    assert(extractor.`type`(json) === expected)
  }

  it should "create the correct DPLA URI" in {
    val expected = Some(URI("http://dp.la/api/items/9e0a3ebaa06174954095aabba527f15f"))
    assert(extractor.dplaUri(json) === expected)
  }

  it should "extract the correct IIIF URI" in {
    val expected = Seq(URI("https://cdm16022.contentdm.oclc.org/iiif/p16022coll518:690/manifest.json"))
    assert(extractor.iiifManifest(json) === expected)
  }
}
