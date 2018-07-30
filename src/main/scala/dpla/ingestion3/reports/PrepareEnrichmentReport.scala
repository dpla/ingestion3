package dpla.ingestion3.reports

import dpla.ingestion3.messages.{IngestMessage, IngestMessageTemplates, MessageCollector}
import dpla.ingestion3.model.{DplaPlace, OreAggregation}
import dpla.ingestion3.reports.summary.ReportFormattingUtils
import dpla.ingestion3.utils.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

object PrepareEnrichmentReport extends IngestMessageTemplates {

  def generateFieldReport(ds: Dataset[Row], field: String) = {
    val queryResult = ds
      .select("level", "message", "id", "field")
      .where(s"field=='$field'")
      .drop("field")
      .groupBy("level", "message")
      .agg(count("id")).as("count")
      .orderBy("level","message")
      .orderBy(desc("count(id)"))

    queryResult.collect().map(row => ReportFormattingUtils
      .centerPad("- " + row(1).toString.trim, Utils.formatNumber(row.getLong(2))))
      .mkString("\n")
  }

  /**
    *
    * @param enriched
    * @param original
    * @param msgs
    * @return
    */
  def prepareEnrichedData(enriched: OreAggregation, original: OreAggregation)
                         (implicit msgs: MessageCollector[IngestMessage]) = {
    // Get messages for each field
    prepareLangauge(enriched)
    prepareType(original, enriched)
    preparePlace(enriched)

    // messages are correctly collected

    // Put collected messages into copy of enriched
    enriched.copy(messages = msgs.getAll())
  }

  /**
    *
    * @param enriched
    * @param msgs
    * @return
    */
  def prepareLangauge(enriched: OreAggregation)
                     (implicit msgs: MessageCollector[IngestMessage]) = {

    enriched.sourceResource.language.map( l => {
      if(l.concept.getOrElse("") != l.providedLabel.getOrElse(" ")){
        msgs.add(enrichedValue(
          (enriched.sidecar \\ "dplaId").values.toString,
          "language",
          l.providedLabel.getOrElse("AADS"),
          l.concept.getOrElse("SFSFSD")))
      }else
        msgs.add(originalValue(
          (enriched.sidecar \\ "dplaId").values.toString,
          "language",
          l.providedLabel.getOrElse(""))
        )
    })
  }

  /**
    *
    * @param enriched
    * @param msgs
    * @return
    */
  def preparePlace(enriched: OreAggregation)
                     (implicit msgs: MessageCollector[IngestMessage]) = {

    enriched.sourceResource.place.map( p => {
      if(p.city.isDefined | p.coordinates.isDefined | p.country.isDefined | p.region.isDefined | p.state.isDefined) {
        msgs.add( enrichedValue((enriched.sidecar \\ "dplaId").values.toString, "place", p.name.getOrElse(""), printPlace(p)))
      }
    })
  }

  def printPlace(place: DplaPlace) = {
    // TODO How else to format?
    s"""
       |${place.country.getOrElse("")}
       |${place.region.getOrElse("")}
       |${place.state.getOrElse("")}
       |${place.county.getOrElse("")}
       |${place.city.getOrElse("")}
     """.stripMargin.split("\n").filter(_.nonEmpty).mkString("\n")
  }

  /**
    *
    * @param original
    * @param enriched
    * @param msgs
    * @return
    */
  def prepareType(original: OreAggregation, enriched: OreAggregation)
                 (implicit msgs: MessageCollector[IngestMessage]) = {
    val enrichTypeValues = enriched.sourceResource.`type`
    val originalTypeValues = original.sourceResource.`type`

    val typeTuples = enrichTypeValues zip originalTypeValues

    typeTuples.map( { case (e: String, o: String) => {
      if(e != o)
        msgs.add(enrichedValue( (enriched.sidecar \\ "dplaId").values.toString, "type", o, e))
      else
        msgs.add(originalValue( (enriched.sidecar \\ "dplaId").values.toString, "type", o))
    }})
  }
}