package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.{Document, Mapping}
import dpla.ingestion3.model.{DcmiTypeCollection, EdmAgent, EdmTimeSpan, EdmWebResource, URI, nameOnlyAgent, stringOnlyTimeSpan, uriOnlyWebResource}
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JValue
import org.json4s.jackson.JsonMethods
import org.scalatest.FlatSpec
import org.json4s.JsonDSL._

class JsonRetconMappingTest extends FlatSpec {

  def loadRecord(path: String): Document[JValue] = Document(
    JsonMethods.parse(
      new FlatFileIO()
        .readFileAsString(path)
    )
  )

  def runTests[T](testCases: Seq[TestCase[T]]): Unit = {
    for (testCase <- testCases) {
      assert(testCase.mapping(testCase.data) === testCase.expectedValue)
    }
  }


  case class MappingAndData(
                             mapping: JsonRetconMapping,
                             document: Document[JValue]
                           )

  case class TestCase[T](
                          expectedValue: T,
                          mapping: Document[JValue] => T,
                          data: Document[JValue]
                        )

  val artstor: MappingAndData = MappingAndData(
    new ArtstorRetconMapping,
    loadRecord("/json-retcon/artstor.json")
  )

  val kentucky: MappingAndData = MappingAndData(
    new KentuckyRetconMapping,
    loadRecord("/json-retcon/kentucky.json")
  )

  val lc: MappingAndData = MappingAndData(
    new LcRetconMapping,
    loadRecord("/json-retcon/lc.json")
  )

  val maine: MappingAndData = MappingAndData(
    new MaineRetconMapping,
    loadRecord("/json-retcon/maine.json")
  )

  val washington: MappingAndData = MappingAndData(
    new WashingtonRetconMapping,
    loadRecord("/json-retcon/washington.json")
  )

  "A JsonRetconMapping" should "mint the correct DPLA URI" in
    runTests(
      Seq(
        TestCase(
          Some(URI("http://dp.la/api/items/997cbfdb65b54b965d1197968d39083f")),
          artstor.mapping.dplaUri,
          artstor.document
        ),
        TestCase(
          Some(URI("http://dp.la/api/items/7781509fd5609a093a5c58dcf5017d4d")),
          kentucky.mapping.dplaUri,
          kentucky.document
        ),
        TestCase(
          Some(URI("http://dp.la/api/items/8bca6ea2341791bb4b30c471115e76e8")),
          lc.mapping.dplaUri,
          lc.document
        ),
        TestCase(
          Some(URI("http://dp.la/api/items/4a2b983d11c024989573f3d9b53f5877")),
          maine.mapping.dplaUri,
          maine.document
        ),
        TestCase(
          Some(URI("http://dp.la/api/items/a2a415d0f3030e86e0349f90747d1c86")),
          washington.mapping.dplaUri,
          washington.document
        )
      )
    )

  it should "create the correct sidecar" in {

    def sidecar(preHashId: String, dplaId: String): JValue =
      ("prehashId", preHashId) ~ ("dplaId", dplaId)

    runTests(
      Seq(
        TestCase(
          sidecar(
            "oai:oaicat.oclc.org:SS7729997_7729997_8329558_TRINITY",
            "997cbfdb65b54b965d1197968d39083f"
          ),
          artstor.mapping.sidecar,
          artstor.document
        ),
        TestCase(
          sidecar(
            "http://kdl.kyvl.org/catalog/xt7cnp1wfn5x_1",
            "7781509fd5609a093a5c58dcf5017d4d"
          ),
          kentucky.mapping.sidecar,
          kentucky.document
        ),
        TestCase(
          sidecar(
            "8bca6ea2341791bb4b30c471115e76e8",
            "8bca6ea2341791bb4b30c471115e76e8"
          ),
          lc.mapping.sidecar,
          lc.document
        ),
        TestCase(
          sidecar(
            "oai:the.european.library.msl_statedocs:oai:digitalmaine.com:spo_docs-1067",
            "4a2b983d11c024989573f3d9b53f5877"
          ),
          maine.mapping.sidecar,
          maine.document
        ),
        TestCase(
          sidecar(
            "a2a415d0f3030e86e0349f90747d1c86",
            "a2a415d0f3030e86e0349f90747d1c86"
          ),
          washington.mapping.sidecar,
          washington.document
        )
      )
    )
  }

  it should "extract the correct dataProvider" in
    runTests(
      Seq(
        TestCase(
          Seq(EdmAgent(name = Some("Trinity College"))),
          artstor.mapping.dataProvider,
          artstor.document
        ),
        TestCase(
          Seq(EdmAgent(name = Some("University of Kentucky"))),
          kentucky.mapping.dataProvider,
          kentucky.document
        ),
        TestCase(
          Seq(EdmAgent(name = Some("Library of Congress Geography and Map Division Washington, D.C. 20540-4650 USA dcu"))),
          lc.mapping.dataProvider,
          lc.document
        ),
        TestCase(
          Seq(EdmAgent(name = Some("Maine State Library"))),
          maine.mapping.dataProvider,
          maine.document
        ),
        TestCase(
          Seq(EdmAgent(name = Some("University of Washington"))),
          washington.mapping.dataProvider,
          washington.document
        )
      )
    )

  it should "extract the correct originalRecord" in {
    runTests(
      Seq(
        TestCase(
          f"""{"status":[],"publisher":"Trinity College","handle":["http://www.trincoll.edu/LITC/Library/Pages/default.aspx","http://media.artstor.net/imgstor/size1/sslps/c7729997/2842515.jpg","http://search.openlibrary.artstor.org/object/SS7729997_7729997_8329558_TRINITY"],"description":null,"rights":"This digital collection and its contents are made available by Trinity College Library for limited non-commercial, educational and personal use only. For other uses, or for additional information regarding the collection, contact the staff of Watkinson Library (www.watkinsonlibrary.org). Shared Shelf Commons is a free service provided by ARTstor. ARTstor does not screen or select these images, and is acting as an online service provider under the Digital Millennium Copyright act in making this service available. Should you have copyright concerns about an image, please go to the Reporting a Copyright Problem of our website and follow the instructions, which will provide us with the necessary information to proceed.","format":"dimensions of image: 6.99 x 7.62 cm (2.75 x 3 inches) approximately, dimensions of slide: 3.25 x 4 inches approximately; photography; Lantern slide","collection":{"title":"SSDPLATrinity","@id":"http://dp.la/api/collections/e14e690d4bf40114ac139c015bcb4cb7","description":"SSDPLATrinity","id":"e14e690d4bf40114ac139c015bcb4cb7"},"label":"Ring-billed Gulls, Nelson County, North Dakota; overall","date":"May 31, 1901 (creation date)","coverage":"Trinity College, Watkinson Library (Hartford, Connecticut, USA)","provider":{"@id":"http://dp.la/api/contributor/artstor","name":"ARTstor"},"datestamp":"2018-05-01","title":"Ring-billed Gulls, Nelson County, North Dakota; overall","creator":"Job, Herbert Keightley (photographer, American, 1864-1933), American","id":"oai:oaicat.oclc.org:SS7729997_7729997_8329558_TRINITY","setSpec":"SSDPLATrinity","subject":"Ornithology; Lantern slides; Birds -- United States; Gulls, Terns and Skimmers; Birds -- North Dakota"}""",
          artstor.mapping.originalRecord,
          artstor.document
        ),
        TestCase(
          f"""{"status":[],"publisher":"University of Kentucky","handle":"http://kdl.kyvl.org/catalog/xt7cnp1wfn5x_1","language":"english","format":"newspapers","rights":"This digital resource may be freely searched and displayed. Permission must be received for subsequent distribution in print or electronically. Physical rights are retained by the owning repository. Copyright is retained in accordance with U. S. copyright laws. Please go to http://kdl.kyvl.org for more information.","label":"Image 1 of Paducah sun (Paducah, Ky. : 1898), February 20, 1906","contributor":"University of Kentucky","creator":"Paducah sun (Paducah, Ky. : 1898).","datestamp":"2014-06-26T05:57:01Z","relation":"http://nyx.uky.edu/dips/xt7cnp1wfn5x/data/0358/0358.jpg","source":"Paducah sun (Paducah, Ky. : 1898)","coverage":"McCracken County, Kentucky","provider":{"@id":"http://dp.la/api/contributor/kdl","name":"Kentucky Digital Library"},"date":"1906","title":"Image 1 of Paducah sun (Paducah, Ky. : 1898), February 20, 1906","type":"text","id":"kdl.kyvl.org/xt7cnp1wfn5x_1"}""",
          kentucky.mapping.originalRecord,
          kentucky.document
        ),
        TestCase(
          "\"http://ldp.staging.dp.la/ldp/original_record/8bca6ea2341791bb4b30c471115e76e8.json\"",
          lc.mapping.originalRecord,
          lc.document
        ),
        TestCase(
          f"""{"handle":"http://digitalmaine.com/spo_docs/67","setSpec":"msl_statedocs","status":[],"subject":["State Planning Office","recycling","plastics","Maine"],"rights":"http://rightsstatements.org/vocab/NoC-US/1.0/","provider":{"@id":"http://dp.la/api/contributor/maine","name":"Digital Maine"},"label":"Report on the Costs and Benefits of State and Local Options to Stimulate an Increase in the Recycling of Plastics. 2009","collection":{"id":"bfcb89bb223896ab1549ac496606cf83","description":"","title":"Maine State Documents","@id":"http://dp.la/api/collections/bfcb89bb223896ab1549ac496606cf83"},"type":"Text","creator":"Maine State Planning Office","id":"oai:the.european.library.msl_statedocs:oai:digitalmaine.com:spo_docs-1067","title":"Report on the Costs and Benefits of State and Local Options to Stimulate an Increase in the Recycling of Plastics. 2009","created":"2009-01-01","contributor":"Maine State Library","datestamp":"2017-06-20","language":"English"}""",
          maine.mapping.originalRecord,
          maine.document
        ),
        TestCase(
          "\"http://ldp.dp.la/ldp/original_record/a2a415d0f3030e86e0349f90747d1c86.xml\"",
          washington.mapping.originalRecord,
          washington.document
        )
      )
    )
  }

  it should "extract isShownAt" in
    runTests(
      Seq(
        TestCase(Seq(EdmWebResource(uri = URI("http://search.openlibrary.artstor.org/object/SS7729997_7729997_8329558_TRINITY"))), artstor.mapping.isShownAt, artstor.document),
        TestCase(Seq(EdmWebResource(uri = URI("https://exploreuk.uky.edu/catalog/xt7cnp1wfn5x_1"))), kentucky.mapping.isShownAt, kentucky.document),
        TestCase(Seq(EdmWebResource(uri = URI("https://www.loc.gov/item/78695093/"))), lc.mapping.isShownAt, lc.document),
        TestCase(Seq(EdmWebResource(uri = URI("http://digitalmaine.com/spo_docs/67"))), maine.mapping.isShownAt, maine.document),
        TestCase(Seq(EdmWebResource(uri = URI("http://cdm16786.contentdm.oclc.org/cdm/ref/collection/sayre/id/9054"))), washington.mapping.isShownAt, washington.document)
      )
    )

  it should "extract the correct provider" in
    runTests(
      Seq(
        TestCase(
          EdmAgent(
            name = Some("Artstor"),
            uri = Some(URI("http://dp.la/api/contributor/artstor"))
          ),
          artstor.mapping.provider,
          artstor.document
        ),
        TestCase(
          EdmAgent(
            name = Some("Kentucky Digital Library"),
            uri = Some(URI("http://dp.la/api/contributor/kdl"))
          ),
          kentucky.mapping.provider,
          kentucky.document
        ),
        TestCase(
          EdmAgent(
            name = Some("Library of Congress"),
            uri = Some(URI("http://dp.la/api/contributor/lc"))
          ),
          lc.mapping.provider,
          lc.document
        ),
        TestCase(
          EdmAgent(
            name = Some("Digital Maine"),
            uri = Some(URI("http://dp.la/api/contributor/maine"))
          ),
          maine.mapping.provider,
          maine.document
        ),
        TestCase(
          EdmAgent(
            name = Some("University of Washington"),
            uri = Some(URI("http://dp.la/api/contributor/washington"))
          ),
          washington.mapping.provider,
          washington.document
        )
      )
    )

  it should "extract the correct object" in
    runTests(
      Seq(
        TestCase(
          Seq(uriOnlyWebResource(URI("http://media.artstor.net/imgstor/size1/sslps/c7729997/2842515.jpg"))),
          artstor.mapping.`object`,
          artstor.document
        ),
        TestCase(
          Seq(uriOnlyWebResource(URI("http://nyx.uky.edu/dips/xt7cnp1wfn5x/data/0358/0358_tb.jpg"))),
          kentucky.mapping.`object`,
          kentucky.document
        ),
        TestCase(
          Seq(uriOnlyWebResource(URI("http://cdn.loc.gov/service/gmd/gmd376/g3764/g3764n/pm003122.gif"))),
          lc.mapping.`object`,
          lc.document
        ),
        TestCase(
          Seq(),
          maine.mapping.`object`,
          maine.document
        ),
        TestCase(
          Seq(uriOnlyWebResource(uri = URI("http://cdm16786.contentdm.oclc.org/utils/getthumbnail/collection/sayre/id/9054"))),
          washington.mapping.`object`,
          washington.document
        )
      )
    )

  it should "extract the correct edm:Rights" in
    runTests(
      Seq(
        TestCase(Seq(), artstor.mapping.edmRights, artstor.document),
        TestCase(Seq(), kentucky.mapping.edmRights, kentucky.document),
        TestCase(Seq(), lc.mapping.edmRights, lc.document),
        TestCase(Seq(URI("http://rightsstatements.org/vocab/NoC-US/1.0/")), maine.mapping.edmRights, maine.document),
        TestCase(Seq(), washington.mapping.edmRights, washington.document)
      )
    )

  it should "extract collection" in
    runTests(
      Seq(
        TestCase(
          Seq(
            DcmiTypeCollection(
              title = Some("SSDPLATrinity"),
              description = Some("SSDPLATrinity")
            )
          ),
          artstor.mapping.collection,
          artstor.document
        ),
        TestCase(Seq(), kentucky.mapping.collection, kentucky.document),
        TestCase(
          Seq(
            DcmiTypeCollection(
              title = Some("panoramic maps")
            ),
            DcmiTypeCollection(
              title = Some("cities and towns")
            ),
            DcmiTypeCollection(
              title = Some("geography and map division")
            ),
            DcmiTypeCollection(
              title = Some("catalog")
            ),
            DcmiTypeCollection(
              title = Some("american memory")
            )
          ),
          lc.mapping.collection,
          lc.document
        ),
        TestCase(Seq(), maine.mapping.collection, maine.document),
        TestCase(
          Seq(
            DcmiTypeCollection(
              title = Some("sayre")
            )
          ),
          washington.mapping.collection,
          washington.document
        )
      )
    )

  it should "extract contributor" in
    runTests(
      Seq(
        TestCase(Seq(), artstor.mapping.contributor, artstor.document),
        TestCase(Seq(), kentucky.mapping.contributor, kentucky.document),
        TestCase(Seq(nameOnlyAgent("O.H. Bailey & Co.")), lc.mapping.contributor, lc.document),
        TestCase(Seq(), maine.mapping.contributor, maine.document),
        TestCase(Seq(), washington.mapping.contributor, washington.document)
      )
    )

  it should "extract creator" in
    runTests(
      Seq(
        TestCase(
          Seq(
            nameOnlyAgent("Job, Herbert Keightley (photographer, American, 1864-1933), American")
          ),
          artstor.mapping.creator,
          artstor.document
        ),
        TestCase(
          Seq(
            nameOnlyAgent("Paducah sun (Paducah, Ky. : 1898)")
          ),
          kentucky.mapping.creator,
          kentucky.document
        ),
        TestCase(
          Seq(),
          lc.mapping.creator,
          lc.document
        ),
        TestCase(
          Seq(
            nameOnlyAgent("Maine State Planning Office")
          ),
          maine.mapping.creator,
          maine.document
        ),
        TestCase(
          Seq(),
          washington.mapping.creator,
          washington.document
        )
      )
    )

  it should "extract date" in
    runTests(
      Seq(
        TestCase(
          Seq(
            EdmTimeSpan(
              prefLabel = Some("May 31, 1901 (creation date)"),
              originalSourceDate = Some("May 31, 1901 (creation date)")
            )
          ),
          artstor.mapping.date,
          artstor.document
        ),
        TestCase(
          Seq(
            EdmTimeSpan(
              begin = Some("1906"),
              end = Some("1906"),
              originalSourceDate = Some("1906"),
              prefLabel = Some("1906")
            )
          ),
          kentucky.mapping.date,
          kentucky.document
        ),
        TestCase(
          Seq(
            EdmTimeSpan(
              prefLabel = Some("1878"),
              originalSourceDate = Some("1878"),
              begin = Some("1878-01-01"),
              end = Some("1878-12-31")
            )
          ),
          lc.mapping.date,
          lc.document
        ),
        TestCase(
          Seq(
            EdmTimeSpan(
              prefLabel = Some("2009-01-01"),
              originalSourceDate = Some("2009-01-01"),
              begin = Some("2009-01-01"),
              end = Some("2009-01-01")
            )
          ),
          maine.mapping.date,
          maine.document
        ),
        TestCase(
          Seq(
            EdmTimeSpan(
              prefLabel = Some("1917"),
              originalSourceDate = Some("1917"),
              begin = Some("1917-01-01"),
              end = Some("1917-12-31")
            )
          ),
          washington.mapping.date,
          washington.document
        )
      )
    )

  it should "extract description" in
    runTests (
      Seq(
        TestCase(
          Seq(),
          artstor.mapping.description,
          artstor.document
        ),
        TestCase(
          Seq(),
          kentucky.mapping.description,
          kentucky.document
        ),
        TestCase(
          Seq(
            "Perspective map not drawn to scale. LC Panoramic maps (2nd ed.), 312.2 Available also through the Library of Congress Web site as a raster image. Indexed for points of interest. AACR2",
            "Boston : The Co., 1878."
          ),
          lc.mapping.description,
          lc.document
        ),
        TestCase(
          Seq(),
          maine.mapping.description,
          maine.document
        ),
        TestCase(
          Seq(
            "Valeska Suratt, silent film actress"
          ),
          washington.mapping.description,
          washington.document
        )
      )
    )

  it should "extract extent" in
    runTests (
      Seq(
        TestCase(
          Seq(),
          artstor.mapping.extent,
          artstor.document
        ),
        TestCase(
          Seq(),
          kentucky.mapping.extent,
          kentucky.document
        ),
        TestCase(
          Seq(),
          lc.mapping.extent,
          lc.document
        ),
        TestCase(
          Seq(),
          maine.mapping.extent,
          maine.document
        ),
        TestCase(
          Seq(),
          washington.mapping.extent,
          washington.document
        )
      )
    )

  it should "extract format" in
    runTests (
      Seq(
        TestCase(
          Seq(
            "Dimensions of image: 6.99 x 7.62 cm (2.75 x 3 inches) approximately, dimensions of slide: 3.25 x 4 inches approximately",
            "Photography",
            "Lantern slide"
          ),
          artstor.mapping.format,
          artstor.document
        ),
        TestCase(
          Seq(
            "Newspapers"
          ),
          kentucky.mapping.format,
          kentucky.document
        ),
        TestCase(
          Seq(
            "map"
          ),
          lc.mapping.format,
          lc.document
        ),
        TestCase(
          Seq(),
          maine.mapping.format,
          maine.document
        ),
        TestCase(
          Seq(
            "image",
            "photograph",
            "Scanned at 600ppi with an Epson 20000 flatbed scanner. Image then rotated, cropped, level-adjusted, and sharpened using Photoshop CS3. Converted to a JPEG2000 image upon ingest into CONTENTdm"
          ),
          washington.mapping.format,
          washington.document
        )
      )
    )

  it should "extract genre" in
    runTests (
      Seq(
        TestCase(Seq(), artstor.mapping.genre, artstor.document),
        TestCase(Seq(), kentucky.mapping.genre, kentucky.document),
        TestCase(Seq(), lc.mapping.genre, lc.document),
        TestCase(Seq(), maine.mapping.genre, maine.document),
        TestCase(Seq(), washington.mapping.genre, washington.document)
      )
    )

  /*
  def foo() = {
  runTests (
    Seq(
      TestCase(, artstor.mapping., artstor.document),
      TestCase(, kentucky.mapping., kentucky.document),
      TestCase(, lc.mapping., lc.document),
      TestCase(, maine.mapping., maine.document),
      TestCase(, washington.mapping., washington.document)
    )
  )

   */
}


