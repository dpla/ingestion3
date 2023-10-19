package dpla.ingestion3.data

import dpla.ingestion3.model._
import org.json4s.JsonDSL._
import dpla.ingestion3.wiki.WikiUri
object EnrichedRecordFixture {

  val wikimediaEntrichedRecord =
    OreAggregation(
      dataProvider = EdmAgent(
        name = Some("Big Sky Digital Network"),
        exactMatch = Seq(URI(s"${WikiUri.baseWikiUri}Q83878447"))
      ),
      dplaUri = URI("https://dp.la/item/123"),
      originalRecord = "The Original Record",
      provider = EdmAgent(
        uri = Some(URI("http://dp.la/api/contributor/bscdn")),
        name = Some("Big Sky Digital Network"),
        exactMatch = Seq(URI(s"${WikiUri.baseWikiUri}Q83878447"))
      ),
      intermediateProvider = Some(
        EdmAgent(name = Some("The Intermediate Provider"))
      ),
      `object` = Some(
        EdmWebResource(uri = URI("https://example.org/record/123.html"))
      ),
      preview = Some(
        EdmWebResource(uri = URI("https://example.org/thumbnail/123.jpg"))
      ),
      edmRights = Some(URI("http://rightsstatements.org/vocab/NoC-US/")),
      isShownAt = EdmWebResource(
        uri = URI("https://example.org/record/123")
      ),
      sidecar = toJsonString(("prehashId" -> "oai:somestate:id123") ~ ("dplaId" -> "4b1bd605bd1d75ee23baadb0e1f24457")),
      originalId = "The original ID",
      sourceResource = DplaSourceResource(
        creator = Seq(EdmAgent(
          name = Some("J Doe")
        )),
        date = Seq(EdmTimeSpan(
          originalSourceDate = Some("5.7.2012"),
          prefLabel = Some("2012-05-07"),
          begin = Some("2012-05-07"),
          end = Some("2012-05-07")
        )),
        description = Seq("The description"),
        identifier = Seq("us-history-13243", "j-doe-archives-2343"),
        title = Seq("The Title"),
        `type` = Seq("image", "text")
      ),
      iiifManifest = Some(URI("https://ark.iiif/item/manifest")),
      mediaMaster = Seq(EdmWebResource(uri = URI("https://example.org/record/123.html")))
    )

  val enrichedRecord =
    OreAggregation(
      dataProvider = EdmAgent(
        name = Some("The Data Provider"),
        uri = Some(URI("http://dp.la/api/contributor/the-data-provider")),
        exactMatch = Seq(URI("Q83878447"))
      ),
      dplaUri = URI("https://dp.la/item/123"),
      originalRecord = "The Original Record",
      provider = EdmAgent(
        uri = Some(URI("http://dp.la/api/contributor/thedataprovider")),
        name = Some("The Provider")
      ),
      hasView = Seq(EdmWebResource(uri = URI("https://example.org/"))),
      intermediateProvider = Some(
        EdmAgent(name = Some("The Intermediate Provider"))
      ),
      `object` = Some(
        EdmWebResource(uri = URI("https://example.org/record/123.html"))
      ),
      preview = Some(
        EdmWebResource(uri = URI("https://example.org/thumbnail/123.jpg"))
      ),
      edmRights = Some(URI("https://example.org/rights/public_domain.html")),
      isShownAt = EdmWebResource(
        uri = new URI("https://example.org/record/123")
      ),
      sidecar = toJsonString(("prehashId" -> "oai:somestate:id123") ~ ("dplaId" -> "4b1bd605bd1d75ee23baadb0e1f24457")),
      originalId = "The original ID",
      sourceResource = DplaSourceResource(
        collection = Seq(DcmiTypeCollection(
          title = Some("The Collection"),
          description = Some("The Archives of Some Department, U. of X"),
          isShownAt = Some(stringOnlyWebResource("http://catalog.archives.gov/id/123"))
        )),
        contributor = Seq(EdmAgent(
          name = Some("J Doe")
        )),
        creator = Seq(EdmAgent(
          name = Some("J Doe")
        )),
        date = Seq(EdmTimeSpan(
          originalSourceDate = Some("5.7.2012"),
          prefLabel = Some("2012-05-07"),
          begin = Some("2012-05-07"),
          end = Some("2012-05-07")
        )),
        description = Seq("The description"),
        extent = Seq("200 pp.", "8 x 10 in."),
        format = Seq("handwritten letter", "gelatin silver print"),
        identifier = Seq("us-history-13243", "j-doe-archives-2343"),
        language = Seq(SkosConcept(
          providedLabel = Some("English"),
          concept = Some("eng"),
          note = None,
          scheme = None,
          exactMatch = Seq(),
          closeMatch = Seq())
        ),
        publisher = Seq(EdmAgent(
          name = Some("The Publisher")
        )),
        relation = Seq("x", "https://example.org/x").map(eitherStringOrUri),
        rights = Seq("Public Domain"),
        subject = Seq(SkosConcept(
          providedLabel = Some("Birds"),
          exactMatch = Seq(URI("http://loc.gov/id/123"))
        )),
        temporal = Seq(
          EdmTimeSpan(
            originalSourceDate = Some("1920s"),
            begin = Some("1920-01-01"),
            end = Some("1929-12-31")
          ),
          EdmTimeSpan(
            originalSourceDate = Some("1930s"),
            begin = Some("1930-01-01"),
            end = Some("1939-12-31")
          )
        ),
        title = Seq("The Title"),
        `type` = Seq("image", "text")
      ),
      tags = Seq(URI("tag")),
      iiifManifest = Some(URI("https://ark.iiif/item/manifest")),
      mediaMaster = Seq(EdmWebResource(uri = URI("https://example.org/record/123.html")))
    )

  val minimalEnrichedRecord =
    OreAggregation(
      sidecar = toJsonString(("prehashId" -> "oai:somestate:id123") ~ ("dplaId" -> "123")),
      sourceResource = DplaSourceResource(
        rights = Seq("Public Domain"),
        title = Seq("The Title"),
        `type` = Seq("image", "text")
      ),
      dataProvider = EdmAgent(
        name = Some("The Data Provider"),
        uri = Some(URI("http://dp.la/api/contributor/the-data-provider"))
      ),
      dplaUri = URI("https://dp.la/item/123"),
      isShownAt = EdmWebResource(uri = URI("https://example.org/record/123")),
      originalRecord = "The Original Record",
      provider = EdmAgent(
        name = Some("The Provider"),
        uri = Some(URI("http://dp.la/api/contributor/thedataprovider"))
      ),
      hasView = Seq(EdmWebResource(uri = URI("https://example.org/"))),
      intermediateProvider = Some(
        EdmAgent(name = Some("The Intermediate Provider"))
      ),
      `object` = Some(
        EdmWebResource(uri = URI("https://example.org/record/123.html"))
      ),
      preview = Some(
        EdmWebResource(uri = URI("https://example.org/thumbnail/123.jpg"))
      ),
      edmRights = Some(URI("https://example.org/rights/public_domain.html")),
      originalId = "The original ID"
    )
}
