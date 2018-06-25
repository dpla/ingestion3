package dpla.ingestion3.reports

import dpla.ingestion3.messages.{IngestMessage, IngestMessageTemplates, MessageCollector}
import dpla.ingestion3.model.OreAggregation
import dpla.ingestion3.reports.summary.ReportFormattingUtils
import dpla.ingestion3.utils.Utils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}

object PrepareEnrichmentReport extends IngestMessageTemplates {
  def generateReport(dataset: Dataset[Row]) = {

    val languageSummary = generateLanguageReport(dataset)
  }

  /**
    *
    * @param ds
    * @return
    */
  def generateLanguageReport(ds: Dataset[Row]) = {
    val t = ds
      .select("level", "message", "id", "field")
      .where("field=='language'")
      .drop("field")
      .groupBy("level", "message")
      .agg(count("id")).as("count")
      .orderBy("level","message")
      .orderBy(desc("count(id)"))

    t.collect().map(row => ReportFormattingUtils
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
    prepareLangauge(enriched)
    prepareType(original, enriched)
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
      if(l.concept.getOrElse("") != l.providedLabel.getOrElse(" "))
        msgs.add(enrichedValue(
          (enriched.sidecar \\ "dplaId").values.toString,
          "language",
          l.providedLabel.getOrElse(""),
          l.concept.getOrElse("")))
      else
        msgs.add(originalValue(
          (enriched.sidecar \\ "dplaId").values.toString,
          "language",
          l.providedLabel.getOrElse("")))
    })
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
    val enrichTypeValue: String = enriched.sourceResource.`type`.headOption.getOrElse("")
    val originalTypeValue: String = original.sourceResource.`type`.headOption.getOrElse("")

    (enrichTypeValue == originalTypeValue, originalTypeValue.nonEmpty) match {
      case (false, true) => // valid enrichment orig <> enrich and orig is not empty
        msgs.add(enrichedValue(
          (enriched.sidecar \\ "dplaId").values.toString,
          "type",
          original.sourceResource.`type`.headOption.getOrElse(""),
          enriched.sourceResource.`type`.headOption.getOrElse("")))
      case (false, false) => // not a valid enrichment b/c it is empty
      case (_,_) => // doesn't matter, no enrichment
    }

  }
}