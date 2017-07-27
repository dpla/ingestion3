package dpla.ingestion3.data

import java.net.URI

import dpla.ingestion3.model._

object MappedRecordsFixture {
  val mappedRecord = DplaMapData(
    new DplaSourceResource(
      date = Seq(EdmTimeSpan(
        originalSourceDate=Some("5.7.2012"),
        prefLabel = None,
        begin = None,
        end = None
      )),
      language = Seq(SkosConcept(
        providedLabel = Some("eng"),
        note = None,
        scheme = None,
        exactMatch= Seq(),
        closeMatch = Seq())
      )
    ),
    EdmWebResource(
      uri = new URI("")
    ),
    new OreAggregation(
      dataProvider = new EdmAgent(),
      uri = new URI(""), //uri of the record on our site
      originalRecord = "", //map v4 specifies this as a ref, but that's LDP maybe?
      provider = new EdmAgent()
    )
  )
}
