package dpla.ingestion3.data

import dpla.ingestion3.model._

object MappedRecordsFixture {

  val mappedRecord = OreAggregation(
    dataProvider = EdmAgent(
      uri = Some(new URI("http://example.com"))
    ),
    dplaUri = new URI(""), //uri of the record on our site
    originalRecord = "", //map v4 specifies this as a ref, but that's LDP maybe?
    provider = EdmAgent(),
    isShownAt = EdmWebResource(uri = new URI("http:/example.com/foo")),
    sourceResource = DplaSourceResource(
      date = Seq(EdmTimeSpan(
        originalSourceDate = Some("5.7.2012"),
        prefLabel = None,
        begin = None,
        end = None
      )),
      language = Seq(SkosConcept(
        providedLabel = Some("eng"),
        note = None,
        scheme = None,
        exactMatch = Seq(),
        closeMatch = Seq())
      ),
      `type` = Seq("audio")
    )
  )
}
