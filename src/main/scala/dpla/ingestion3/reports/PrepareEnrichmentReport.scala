package dpla.ingestion3.reports

import dpla.ingestion3.messages.{
  IngestMessage,
  IngestMessageTemplates,
  MessageCollector
}
import dpla.ingestion3.model.{
  DplaPlace,
  EdmTimeSpan,
  OreAggregation,
  fromJsonString
}
import dpla.ingestion3.reports.summary.ReportFormattingUtils
import dpla.ingestion3.utils.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

object PrepareEnrichmentReport extends IngestMessageTemplates {

  def generateFieldReport(ds: Dataset[Row], field: String): String = {
    val queryResult = ds
      .select("level", "message", "id", "field")
      .where(s"field=='$field'")
      .drop("field")
      .groupBy("level", "message")
      .agg(count("id"))
      .as("count")
      .orderBy("level", "message")
      .orderBy(desc("count(id)"))

    queryResult
      .collect()
      .map(row =>
        ReportFormattingUtils
          .centerPad(
            "- " + row(1).toString.trim,
            Utils.formatNumber(row.getLong(2))
          )
      )
      .mkString("\n")
  }

  /** @param enriched
    * @param original
    * @param msgs
    * @return
    */
  def prepareEnrichedData(enriched: OreAggregation, original: OreAggregation)(
      implicit msgs: MessageCollector[IngestMessage]
  ): OreAggregation = {
    // Get messages for each field
    prepareLanguage(enriched)
    prepareType(original, enriched)
    preparePlace(enriched)
    prepareDate(original, enriched)
    prepareDataProvider(enriched)
    prepareProvider(enriched)

    // messages are correctly collected

    // Put collected messages into copy of enriched
    enriched.copy(messages = msgs.getAll.toSeq)
  }

  private def prepareProvider(
      enriched: OreAggregation
  )(implicit msgs: MessageCollector[IngestMessage]): Unit = {

    val dplaId = (fromJsonString(enriched.sidecar) \\ "dplaId").values.toString

    if (enriched.provider.exactMatch.nonEmpty) {
      msgs.add(
        enrichedValue(
          dplaId,
          "provider.exactMatch.URI",
          enriched.provider.name.getOrElse(""),
          enriched.provider.exactMatch.map(_.toString).mkString(" | ")
        )
      )
    } else {
      msgs.add(
        originalValue(
          dplaId,
          "provider.exactMatch.URI",
          enriched.provider.name.getOrElse("")
        )
      )
    }
  }

  /** @param enriched
    * @param msgs
    */
  private def prepareDataProvider(
      enriched: OreAggregation
  )(implicit msgs: MessageCollector[IngestMessage]): Unit = {

    val dplaId = (fromJsonString(enriched.sidecar) \\ "dplaId").values.toString

    if (enriched.dataProvider.exactMatch.nonEmpty) {
      msgs.add(
        enrichedValue(
          dplaId,
          "dataProvider.exactMatch.URI",
          enriched.dataProvider.name.getOrElse(""),
          enriched.dataProvider.exactMatch.map(_.toString).mkString(" | ")
        )
      )
    } else {
      msgs.add(
        originalValue(
          dplaId,
          "dataProvider.exactMatch.URI",
          enriched.dataProvider.name.getOrElse("")
        )
      )
    }
  }

  /** @param original
    * @param enriched
    * @param msgs
    * @return
    */
  private def prepareDate(original: OreAggregation, enriched: OreAggregation)(implicit
                                                                              msgs: MessageCollector[IngestMessage]
  ) : Unit = {

    val enrichDateValues = enriched.sourceResource.date
    val originalDateValues = original.sourceResource.date
    val id = (fromJsonString(enriched.sidecar) \\ "dplaId").values.toString

    val dateTuples = enrichDateValues zip originalDateValues

    dateTuples.map({ case (e: EdmTimeSpan, o: EdmTimeSpan) =>
      if (e.begin != o.begin || e.end != o.end) { // if the begin and end dates don't match then assume the record was improved
        msgs.add(
          enrichedValue(
            id, // id
            "date", // field
            o.originalSourceDate.getOrElse(""), // original value
            s"begin=${e.begin.getOrElse("")} end=${e.end.getOrElse("")}" // enriched values
          )
        )
      } else
        msgs.add(
          originalValue(
            id, // id
            "date", // field
            o.originalSourceDate.getOrElse("") // original value
          )
        )
    })
  }

  private def prepareLanguage(
      enriched: OreAggregation
  )(implicit msgs: MessageCollector[IngestMessage]) = {

    enriched.sourceResource.language.map(l => {
      if (l.concept.getOrElse("") != l.providedLabel.getOrElse("")) {
        msgs.add(
          enrichedValue(
            (fromJsonString(enriched.sidecar) \\ "dplaId").values.toString,
            "language",
            l.providedLabel.getOrElse(""),
            l.concept.getOrElse("")
          )
        )
      } else
        msgs.add(
          originalValue(
            (fromJsonString(enriched.sidecar) \\ "dplaId").values.toString,
            "language",
            l.providedLabel.getOrElse("")
          )
        )
    })
  }

  private def preparePlace(
      enriched: OreAggregation
  )(implicit msgs: MessageCollector[IngestMessage]): Seq[Any] = {

    enriched.sourceResource.place.map(p => {
      if (
        p.city.isDefined | p.coordinates.isDefined | p.country.isDefined | p.region.isDefined | p.state.isDefined
      ) {
        msgs.add(
          enrichedValue(
            (fromJsonString(enriched.sidecar) \\ "dplaId").values.toString,
            "place",
            p.name.getOrElse(""),
            printPlace(p)
          )
        )
      }
    })
  }

  private def printPlace(place: DplaPlace) = {
    s"""
       |${place.country.getOrElse("")}
       |${place.region.getOrElse("")}
       |${place.state.getOrElse("")}
       |${place.county.getOrElse("")}
       |${place.city.getOrElse("")}
     """.stripMargin.split("\n").filter(_.nonEmpty).mkString("\n")
  }

  private def prepareType(original: OreAggregation, enriched: OreAggregation)
                         (implicit msgs: MessageCollector[IngestMessage]): Unit = {
    val enrichTypeValues = enriched.sourceResource.`type`
    val originalTypeValues = original.sourceResource.`type`

    val typeTuples = enrichTypeValues zip originalTypeValues

    typeTuples.map({
      case (e: String, o: String) =>
        if (e != o)
          msgs.add(
            enrichedValue(
              (fromJsonString(enriched.sidecar) \\ "dplaId").values.toString,
              "type",
              o,
              e
            )
          )
        else
          msgs.add(
            originalValue(
              (fromJsonString(enriched.sidecar) \\ "dplaId").values.toString,
              "type",
              o
            )
          )
    })
  }
}
