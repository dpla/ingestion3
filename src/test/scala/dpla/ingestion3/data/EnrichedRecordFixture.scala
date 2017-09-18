package dpla.ingestion3.data

import java.net.URI

import dpla.ingestion3.model._

object EnrichedRecordFixture {

  val enrichedRecord = DplaMapData(
    DplaSourceResource(
      collection = Seq(DcmiTypeCollection(
        title = Some("The Collection"),
        description = Some("The Archives of Some Department, U. of X")
      )),
      contributor = Seq(EdmAgent(
        name = Some("J Doe")
      )),
      creator = Seq(EdmAgent(
        name = Some("J Doe")
      )),
      date = Seq(EdmTimeSpan(
        originalSourceDate=Some("5.7.2012"),
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
        exactMatch= Seq(),
        closeMatch = Seq())
      ),
      publisher = Seq(EdmAgent(
        name = Some("The Publisher")
      )),
      relation = Seq("x", "https://example.org/x").map(eitherStringOrUri),
      rights = Seq("Public Domain"),
      subject = Seq(SkosConcept(
        providedLabel = Some("Birds")
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
      place = Seq(
        DplaPlace(
          name = Some("Somerville, MA, United States"),
          city = Some("Somerville"),
          county = Some("Middlesex County"),
          country = Some("United States"),
          coordinates = Some("42.3876,-71.0995")
        ),
        DplaPlace(
          name = Some("California"),
          country = Some("United States")
          // Unspecified fields verify serialization of places without optional
          // fields.
        )
      ),
      title = Seq("The Title"),
      `type` = Seq("image", "text")
    ),
    EdmWebResource(
      uri = new URI("https://example.org/record/123")
    ),
    OreAggregation(
      dataProvider = EdmAgent(
        name = Some("The Data Provider")
      ),
      uri = new URI("https://example.org/record/123"),
      originalRecord = "The Original Record",
      provider = EdmAgent(
        uri = Some(new URI("http://dp.la/api/contributor/thedataprovider")),
        name = Some("The Provider")
      ),
      hasView = Seq(EdmWebResource(uri = new URI("https://example.org/"))),
      intermediateProvider = Some(
        EdmAgent(name = Some("The Intermediate Provider"))
      ),
      `object` = Some(
        EdmWebResource(uri = new URI("https://example.org/record/123.html"))
      ),
      preview = Some(
        EdmWebResource(uri = new URI("https://example.org/thumbnail/123.jpg"))
      ),
      edmRights = Some(new URI("https://example.org/rights/public_domain.html"))
    )
  )

  val minimalEnrichedRecord = DplaMapData(
    DplaSourceResource(
      rights = Seq("Public Domain"),
      title = Seq("The Title"),
      `type` = Seq("image", "text")
    ),
    EdmWebResource(
      uri = new URI("https://example.org/record/123")
    ),
    OreAggregation(
      dataProvider = EdmAgent(
        name = Some("The Data Provider")
      ),
      uri = new URI(""),
      originalRecord = "The Original Record",
      provider = EdmAgent(
        name = Some("The Provider"),
        uri = Some(new URI("http://dp.la/api/contributor/thedataprovider"))
      ),
      hasView = Seq(EdmWebResource(uri = new URI("https://example.org/"))),
      intermediateProvider = Some(
        EdmAgent(name = Some("The Intermediate Provider"))
      ),
      `object` = Some(
        EdmWebResource(uri = new URI("https://example.org/record/123.html"))
      ),
      preview = Some(
        EdmWebResource(uri = new URI("https://example.org/thumbnail/123.jpg"))
      ),
      edmRights = Some(new URI("https://example.org/rights/public_domain.html"))
    )
  )

}
