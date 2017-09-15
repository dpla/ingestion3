package dpla.ingestion3

import java.net.URI

import com.databricks.spark.avro.SchemaConverters
import dpla.ingestion3.model.DplaMapData.LiteralOrUri
import dpla.ingestion3.utils.FlatFileIO
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import dpla.ingestion3.utils.Utils.generateMd5
import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType


package object model {

  val DcmiTypes =
    Set("collection", "dataset", "event", "image", "interactiveresource", "service", "software", "sound", "text")

  def nameOnlyAgent(string: String): EdmAgent = EdmAgent(name = Some(string))

  def nameOnlyPlace(string: String): DplaPlace = DplaPlace(name = Some(string))

  def stringOnlyTimeSpan(string: String): EdmTimeSpan = EdmTimeSpan(originalSourceDate = Option(string))

  def nameOnlyConcept(string: String): SkosConcept = SkosConcept(concept = Some(string))

  def nameOnlyCollection(string: String): DcmiTypeCollection = DcmiTypeCollection(title = Some(string))

  def uriOnlyWebResource(uri: URI): EdmWebResource = EdmWebResource(uri = uri)

  def isDcmiType(string: String): Boolean = DcmiTypes.contains(string.toLowerCase.replaceAll(" ", ""))

  def eitherStringOrUri(string: String): LiteralOrUri = new Left(string)

  def eitherStringOrUri(uri: URI): LiteralOrUri = new Right(uri)

  def jsonlRecord(record: DplaMapData): String = {
    val recordID: String = generateMd5(Some(record.oreAggregation.uri.toString))
    val jobj: JObject =
      ("id" -> recordID) ~
      ("@context" -> "http://dp.la/api/items/context") ~
      ("@id" -> ("http://dp.la/api/items/" + recordID)) ~
      ("admin" ->
        ("sourceResource" -> ("title" -> record.sourceResource.title))) ~
      ("aggregatedCHO" -> "#sourceResource") ~
      ("dataProvider" -> record.oreAggregation.dataProvider.name) ~
      ("ingestDate" -> "FIXME") ~  // FIXME: no place in MAPv4 schema for this
      ("ingestType" -> "item") ~
      ("isShownAt" -> record.edmWebResource.uri.toString) ~
      ("object" -> record.oreAggregation.`object`
                         .map{o => o.uri.toString}) ~
      ("originalRecord" -> record.oreAggregation.originalRecord) ~
      ("provider" -> record.oreAggregation.provider.name) ~
      ("sourceResource" ->
        ("@id" ->
          ("http://dp.la/api/items/" + recordID + "#SourceResource")) ~
        ("collection" ->
          record.sourceResource.collection.map {c => "title" -> c.title}) ~
        ("contributor" -> record.sourceResource.contributor.map{c => c.name}) ~
        ("creator" -> record.sourceResource.creator.map{c => c.name}) ~
        ("date" ->
          record.sourceResource.date.map { d =>
            ("displayDate" -> d.originalSourceDate) ~
              ("begin" -> d.begin) ~
              ("end" -> d.end)
          }) ~
        ("description" -> record.sourceResource.description) ~
        ("extent" -> record.sourceResource.extent) ~
        ("format" -> record.sourceResource.format) ~
        ("identifier" -> record.sourceResource.identifier) ~
        ("language" ->
          record.sourceResource.language.map { lang =>
            ("name" -> lang.providedLabel) ~ ("iso639_3" -> lang.concept)
          }) ~
        ("publisher" -> record.sourceResource.publisher.map{p => p.name}) ~
        ("relation" ->
          record.sourceResource.relation.map { r =>
            r.merge.toString
          })  ~
        ("rights" -> record.sourceResource.rights) ~
        // FIXME: Wait til open PR for geo enrichments gets merged before
        // implementing Spatial.
        ("spatial" -> "FIXME") ~
        /*
         * FIXME: specType is unaccounted for in MAPv4 and DplaMapData.
         * It was edm:hasType in MAPv3.1.  It was optional. Shall we omit it?
         */
        ("specType" -> "FIXME") ~
        // stateLocatedIn is being omitted here ...
        ("subject" -> record.sourceResource.subject.map{s => s.providedLabel}) ~
        ("temporal" ->
          record.sourceResource.temporal.map{ t =>
            ("displayDate" -> t.originalSourceDate) ~
              ("begin" -> t.begin) ~
              ("end" -> t.end)
          }) ~
        ("title" -> record.sourceResource.title) ~
        ("type" -> record.sourceResource.`type`)
      ) ~
      ("type" -> "ore:Aggregation")

    compact(render(jobj))
  }

  val avroSchema: Schema = new Schema.Parser().parse(new FlatFileIO().readFileAsString("/avro/MAPRecord.avsc"))
  val sparkSchema: StructType = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]

}
