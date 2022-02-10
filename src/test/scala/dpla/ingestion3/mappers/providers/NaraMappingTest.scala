
package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.messages.{IngestMessage, MessageCollector}
import dpla.ingestion3.model._
import dpla.ingestion3.utils.FlatFileIO
import org.scalatest.{BeforeAndAfter, FlatSpec}

import scala.xml.{NodeSeq, XML}

class NaraMappingTest extends FlatSpec with BeforeAndAfter {

  implicit val msgCollector: MessageCollector[IngestMessage] = new MessageCollector[IngestMessage]
  val shortName = Some("nara")
  val xmlString: String = new FlatFileIO().readFileAsString("/nara.xml")
  val xml: Document[NodeSeq] = Document(XML.loadString(xmlString))
  val itemUri = new URI("http://catalog.archives.gov/id/2132862")
  val extractor = new NaraMapping

  it should "use the provider shortname in minting IDs" in
    assert(extractor.useProviderName)

  it should "pass through the short name to ID minting" in
    assert(extractor.getProviderName === shortName)

  it should "extract original ID" in {
    assert(extractor.originalId(xml) === Some("2132862"))
  }

  it should "construct the correct item uri" in
    assert(extractor.itemUri(xml) === itemUri)

  it should "have the correct DPLA ID" in {
    val dplaUri = extractor.dplaUri(xml)
    assert(dplaUri === Some(URI("http://dp.la/api/items/805598afebf2c093272a5a044938be59")))
  }

  it should "express the right hub details" in {
    val agent = extractor.provider(xml)
    assert(agent.name === Some("National Archives and Records Administration"))
    assert(agent.uri === Some(URI("http://dp.la/api/contributor/nara")))
  }

  it should "extract collections" in {
    val collections = extractor.collection(xml)
    assert(collections === Seq("Records of the Forest Service").map(nameOnlyCollection))
  }

  it should "extract contributors" in {
    val contributors = extractor.contributor(xml)
    assert(contributors === Seq("Department of the Navy. Fourteenth Naval District. Naval Air Station, Pearl Harbor (Hawaii). ca. 1940-9/1947").map(nameOnlyAgent))
  }

  it should "extract creators" in {
    val creators = extractor.creator(xml)
    assert(creators === Seq("Department of Agriculture. Forest Service. Region 9 (Eastern Region). 1965-").map(nameOnlyAgent))
  }

  //todo better coverage of date possibilities?
  it should "extract dates" in {
    val dates = extractor.date(xml)
    assert(dates === Seq(stringOnlyTimeSpan("1967-10")))
  }

  it should "extract descriptions" in {
    val descriptions = extractor.description(xml)
    assert(descriptions === Seq("Original caption: Aerial view of Silver Island Lake, from inlet, looking north, with Perent Lake in background."))
  }

  it should "extract extents" in {
    val extents = extractor.extent(xml)
    assert(extents === Seq("14 pages"))
  }

  it should "extract formats" in {
    val formats = extractor.format(xml)
    assert(formats === Seq("Aerial views", "Photographic Print"))
  }

  it should "extract identifiers" in {
    val identifiers = extractor.identifier(xml)
    assert(identifiers === Seq("2132862"))
  }

  it should "extract languages" in {
    val languages = extractor.language(xml)
    assert(languages === Seq(nameOnlyConcept("Japanese")))
  }

  it should "extract places" in {
    val places = extractor.place(xml)
    assert(places === Seq(nameOnlyPlace("Superior National Forest (Minn.)")))
  }

  //todo can't find publishers
  it should "extract publishers" in {
    val publishers = extractor.publisher(xml)
    assert(publishers === Seq())
  }

  it should "extract relations" in {
    val relations = extractor.relation(xml)
    assert(relations === Seq(Left("Records of the Forest Service ; Historic Photographs")))
  }

  it should "extract rights" in {
    val rights = extractor.rights(xml)
    //todo this mapping is probably wrong in the way it concatenates values
    assert(rights.head.contains("Unrestricted"))
  }

  it should "extract subjects" in {
    val subjects = extractor.subject(xml)
    assert(subjects === Seq(nameOnlyConcept("Recreation"), nameOnlyConcept("Wilderness areas")))
  }

  it should "extract titles" in {
    val titles = extractor.title(xml)
    assert(titles === Seq("Photograph of Aerial View of Silver Island Lake"))
  }

  it should "extract types" in {
    val types = extractor.`type`(xml)
    assert(types.head.contains("image"))
  }

  it should "extract dataProviders" in {
    val dataProvider = extractor.dataProvider(xml)
    assert(dataProvider === Seq(nameOnlyAgent("National Archives at Chicago")))
  }

  it should "contain the hub agent as the provider" in {
    assert(
      extractor.provider(xml) === EdmAgent(
        name = Some("National Archives and Records Administration"),
        uri = Some(URI("http://dp.la/api/contributor/nara"))
      )
    )
  }

  it should "contain the correct isShownAt" in {
    assert(extractor.isShownAt(xml) === Seq(uriOnlyWebResource(itemUri)))
  }

  //todo should we eliminate these default thumbnails?
  it should "find the item previews" in {
    assert(extractor.preview(xml) === Seq(uriOnlyWebResource(URI("https://nara-media-001.s3.amazonaws.com/arcmedia/great-lakes/001/517805_a.jpg"))))
  }

  it should "not map previews with invalid term names" in {
    val xml = <item>
      <naId>51046777</naId>
      <digitalObjectArray>
        <digitalObject>
          <accessFilename>https://nara-media-001.s3.amazonaws.com/arcmedia/great-lakes/001/517805_a.jpg</accessFilename>
          <objectType>
            <termName>NOT VALID</termName>
          </objectType>
        </digitalObject>
      </digitalObjectArray>
    </item>

    assert(extractor.preview(Document(xml)) === Seq())
  }

  it should "correct preview URLs that begin with https://opaexport-conv.s3.amazonaws.com/" in {
    val xml = <item>
      <naId>51046777</naId>
      <digitalObjectArray>
        <digitalObject>
          <accessFilename>https://opaexport-conv.s3.amazonaws.com/TB149/Civil_War_Service_Index/M545-CW_ServRecdIndexUnion_MI/M545_0042/images/2514.jpg</accessFilename>
          <objectType>
            <termName>Image (JPG)</termName>
          </objectType>
        </digitalObject>
      </digitalObjectArray>
    </item>

    val correctUrl = "https://catalog.archives.gov/OpaAPI/media/51046777/content/TB149/Civil_War_Service_Index/M545-CW_ServRecdIndexUnion_MI/M545_0042/images/2514.jpg"
    assert(extractor.preview(Document(xml)) === Seq(uriOnlyWebResource(URI(correctUrl))))
  }

  it should "not map previews that depend on providerIds if providerId is missing" in {
    val xml = <item>
      <digitalObjectArray>
        <digitalObject>
          <accessFilename>https://opaexport-conv.s3.amazonaws.com/TB149/Civil_War_Service_Index/M545-CW_ServRecdIndexUnion_MI/M545_0042/images/2514.jpg</accessFilename>
          <objectType>
            <termName>Image (JPG)</termName>
          </objectType>
        </digitalObject>
      </digitalObjectArray>
    </item>

    assert(extractor.preview(Document(xml)) === Seq())
  }


  it should "map multiple mediaMaster URLs that begin with https://opaexport-conv.s3.amazonaws.com/" in {
    val xml = <item>
      <naId>51046777</naId>
      <digitalObjectArray>
        <digitalObject>
          <accessFilename>https://opaexport-conv.s3.amazonaws.com/TB149/Civil_War_Service_Index/M545-CW_ServRecdIndexUnion_MI/M545_0042/images/2514.jpg</accessFilename>
          <objectType>
            <termName>Image (JPG)</termName>
          </objectType>
        </digitalObject>
        <digitalObject>
          <accessFilename>https://opaexport-conv.s3.amazonaws.com/TB149/Civil_War_Service_Index/M545-CW_ServRecdIndexUnion_MI/M545_0042/images/2516.jpg</accessFilename>
          <objectType>
            <termName>Image (JPG)</termName>
          </objectType>
        </digitalObject>
      </digitalObjectArray>
    </item>

    val correctUrls = Seq("https://catalog.archives.gov/OpaAPI/media/51046777/content/TB149/Civil_War_Service_Index/M545-CW_ServRecdIndexUnion_MI/M545_0042/images/2514.jpg",
      "https://catalog.archives.gov/OpaAPI/media/51046777/content/TB149/Civil_War_Service_Index/M545-CW_ServRecdIndexUnion_MI/M545_0042/images/2516.jpg")
    assert(extractor.preview(Document(xml)) === correctUrls.map(stringOnlyWebResource))
  }


  it should "extract dataProvider from records with fileUnitPhysicalOccurrence" in {
    val xml = <item><physicalOccurrenceArray>
      <fileUnitPhysicalOccurrence>
        <copyStatus>
          <naId>10031434</naId>
          <termName>Preservation-Reproduction-Reference</termName>
        </copyStatus>
        <copyStatusProposal/>
        <locationArray>
          <location>
            <facility>
              <naId>10048490</naId>
              <termName>National Archives Building - Archives I (Washington, DC)</termName>
            </facility>
            <facilityProposal/>
          </location>
        </locationArray>
        <mediaOccurrenceArray>
          <mediaOccurrence>
            <color/>
            <colorProposal/>
            <dimension/>
            <dimensionProposal/>
            <generalMediaTypeArray>
              <generalMediaType>
                <naId>12000005</naId>
                <termName>Loose Sheets</termName>
              </generalMediaType>
            </generalMediaTypeArray>
            <generalMediaTypeProposalArray/>
            <process/>
            <processProposal/>
            <specificMediaType>
              <naId>10048756</naId>
              <termName>Paper</termName>
            </specificMediaType>
            <specificMediaTypeProposal/>
          </mediaOccurrence>
        </mediaOccurrenceArray>
        <referenceUnitArray>
          <referenceUnit>
            <mailCode>RDT1</mailCode>
            <name>National Archives at Washington, DC - Textual Reference</name>
            <address1>National Archives Building</address1>
            <address2>7th and Pennsylvania Avenue NW</address2>
            <city>Washington</city>
            <state>DC</state>
            <postCode>20408</postCode>
            <phone>202-357-5385</phone>
            <fax>202-357-5936</fax>
            <email>Archives1reference@nara.gov</email>
            <naId>32</naId>
            <termName>National Archives at Washington, DC - Textual Reference</termName>
          </referenceUnit>
        </referenceUnitArray>
        <referenceUnitProposalArray/>
      </fileUnitPhysicalOccurrence>
    </physicalOccurrenceArray></item>
  }

  it should "extract dataProvider values" in {
    // DPLA ID 0f7032c62e1cb4b6939694b0a808124c
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <physicalOccurrenceArray>
                  <itemPhysicalOccurrence>
                    <copyStatus>
                      <naId>10031433</naId>
                      <termName>Preservation-Reproduction</termName>
                    </copyStatus>
                    <referenceUnitArray>
                      <referenceUnit>
                        <termName>John F. Kennedy Library</termName>
                      </referenceUnit>
                    </referenceUnitArray>
                  </itemPhysicalOccurrence>
                  <itemPhysicalOccurrence>
                    <copyStatus>
                      <termName>Reference</termName>
                    </copyStatus>
                    <referenceUnitArray>
                      <referenceUnit>
                        <termName>John F. Kennedy Library</termName>
                      </referenceUnit>
                    </referenceUnitArray>
                  </itemPhysicalOccurrence>
                  <itemPhysicalOccurrence>
                    <copyStatus>
                      <termName>Reproduction-Reference</termName>
                    </copyStatus>
                    <referenceUnitArray>
                      <referenceUnit>
                        <termName>John F. Kennedy Library</termName>
                      </referenceUnit>
                    </referenceUnitArray>
                  </itemPhysicalOccurrence>
                </physicalOccurrenceArray>
              </item>

    val dataProvider = extractor.dataProvider(Document(xml))

    assert(Seq(nameOnlyAgent("John F. Kennedy Library")) === dataProvider)
  }

  it should "extract the default dataProvider value if none exist" in {
    // DPLA ID 0f7032c62e1cb4b6939694b0a808124c
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <physicalOccurrenceArray>
                  <itemPhysicalOccurrence>
                    <copyStatus>
                      <naId>10031433</naId>
                      <termName>Preservation-Reproduction</termName>
                    </copyStatus>
                  </itemPhysicalOccurrence>
                  <itemPhysicalOccurrence>
                    <copyStatus>
                      <termName>Reference</termName>
                    </copyStatus>
                  </itemPhysicalOccurrence>
                  <itemPhysicalOccurrence>
                    <copyStatus>
                      <termName>Reproduction-Reference</termName>
                    </copyStatus>
                  </itemPhysicalOccurrence>
                </physicalOccurrenceArray>
              </item>

    val dataProvider = extractor.dataProvider(Document(xml))

    assert(Seq(nameOnlyAgent("National Archives and Records Administration")) === dataProvider)
  }

  it should "extract correct edmRights for Restricted - Fully, Copyright" in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Fully</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Copyright</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>

    val edmRights = extractor.edmRights(Document(xml))

    assert(Seq(URI("http://rightsstatements.org/vocab/InC/1.0/")) === edmRights)
  }

  it should "extract correct edmRights for Undetermined, N/A" in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Undetermined</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>N/A</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>

    val edmRights = extractor.edmRights(Document(xml))

    assert(Seq(URI("http://rightsstatements.org/vocab/CNE/1.0/")) === edmRights)
  }

  it should "work magick for uncertain rites" in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Fully</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Copyright</termName>
                    </specificUseRestriction>
                    <specificUseRestriction>
                      <termName>Other</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>

    val edmRights = extractor.edmRights(Document(xml))

    assert(Seq(URI("http://rightsstatements.org/vocab/InC/1.0/")) === edmRights)
  }

  it should "extract InC for Restricted - Fully && Copyright " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Fully</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Copyright</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>

    val edmRights = extractor.edmRights(Document(xml))

    assert(Seq(URI("http://rightsstatements.org/vocab/InC/1.0/")) === edmRights)
  }

  it should "extract InC for Restricted - Fully && Donor Restrictions " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Fully</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Donor Restrictions</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>

    val edmRights = extractor.edmRights(Document(xml))

    assert(Seq(URI("http://rightsstatements.org/vocab/InC/1.0/")) === edmRights)
  }

  it should "extract InC for Restricted - Fully && Public Law 101-246 " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Fully</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Public Law 101-246</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>

    val edmRights = extractor.edmRights(Document(xml))

    assert(Seq(URI("http://rightsstatements.org/vocab/InC/1.0/")) === edmRights)
  }

  it should "extract NoC-OKLR for Restricted - Fully && Service Mark " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Fully</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Service Mark</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/NoC-OKLR/1.0/")) === edmRights)
  }

  it should "extract NoC-OKLR for Restricted - Fully && Trademark " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Fully</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Trademark</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/NoC-OKLR/1.0/")) === edmRights)
  }

  it should "extract NoC-OKLR for Restricted - Fully && Other " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Fully</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Other</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/NoC-OKLR/1.0/")) === edmRights)
  }

  it should "extract InC for Restricted - Partly && Copyright " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Partly</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Copyright</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/InC/1.0/")) === edmRights)
  }

  it should "extract NoC-OKLR for Restricted - Partly && Donor Restrictions " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Partly</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Donor Restrictions</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/InC/1.0/")) === edmRights)
  }

  it should "extract InC for Restricted - Partly && Public Law 101-246 " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Partly</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Public Law 101-246</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/InC/1.0/")) === edmRights)
  }

  it should "extract NoC-OKLR for Restricted - Partly && Service Mark " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Fully</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Service Mark</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/NoC-OKLR/1.0/")) === edmRights)
  }

  it should "extract NoC-OKLR for Restricted - Partly && Trademark " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Partly</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Trademark</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/NoC-OKLR/1.0/")) === edmRights)
  }

  // *****

  it should "extract NoC-OKLR for Restricted - Partly && Other " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Partly</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Other</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/NoC-OKLR/1.0/")) === edmRights)
  }

  it should "extract UND for Restricted - Possibly && Copyright " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Possibly</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Copyright</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/UND/1.0/")) === edmRights)
  }

  it should "extract UND for Restricted - Possibly && Donor Restrictions " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Possibly</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Donor Restrictions</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/UND/1.0/")) === edmRights)
  }

  it should "extract NKC for Restricted - Possibly && Public Law 101-246 " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Possibly</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Public Law 101-246</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/NKC/1.0/")) === edmRights)
  }

  it should "extract NoC-OKLR for Restricted - Possibly && Service Mark " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Possibly</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Service Mark</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/NoC-OKLR/1.0/")) === edmRights)
  }

  it should "extract NoC-OKLR for Restricted - Possibly && Trademark " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Possibly</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Trademark</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/NoC-OKLR/1.0/")) === edmRights)
  }

  it should "extract NoC-OKLR for `Restricted - Possibly` && `Other` " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Possibly</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Other</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/NoC-OKLR/1.0/")) === edmRights)
  }

  it should "extract UND for `Restricted - Possibly` when no specific use restriction is defined  " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Possibly</termName>
                    </status>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/UND/1.0/")) === edmRights)
  }

  it should "extract CNE for `Undetermined` when no specific use restriction is defined  " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Undetermined</termName>
                    </status>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/CNE/1.0/")) === edmRights)
  }

  it should "extract NoC-US for `Unrestricted` when no specific use restriction is defined  " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Undetermined</termName>
                    </status>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(Seq(URI("http://rightsstatements.org/vocab/CNE/1.0/")) === edmRights)
  }

  it should "extract correct edmRights when given `Unrestricted` use restriction" in {
    val edmRights = extractor.edmRights(xml)
    assert(edmRights === Seq(URI("http://rightsstatements.org/vocab/NoC-US/1.0/")))
  }

  it should "extract no edmRights for `Restricted - Possibly` with competing specific use restrictions that would " +
    "produce NoC-OKLR and NKC rightstatements " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction>
                    <status>
                        <naId>10675400</naId>
                        <termName>Restricted - Possibly</termName>
                    </status>
                  <specificUseRestrictionArray>
                    <specificUseRestriction>
                      <termName>Public Law 101-246</termName>
                    </specificUseRestriction>
                    <specificUseRestriction>
                      <termName>Service Mark</termName>
                    </specificUseRestriction>
                  </specificUseRestrictionArray>
                </useRestriction>
              </item>
    val edmRights = extractor.edmRights(Document(xml))
    assert(edmRights.isEmpty)
  }

  it should "extract no rights value when given an empty block " in {
    val xml = <item xmlns="http://description.das.nara.gov/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
                <useRestriction />
              </item>
    val rights = extractor.rights(Document(xml))
    assert(rights.isEmpty)
  }
}
