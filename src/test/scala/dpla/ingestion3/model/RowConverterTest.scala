package dpla.ingestion3.model

import com.databricks.spark.avro.SchemaConverters
import dpla.ingestion3.data.EnrichedRecordFixture
import dpla.ingestion3.utils.FlatFileIO
import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfter, FlatSpec}

class RowConverterTest extends FlatSpec with BeforeAndAfter {

  val uri1 = URI("http://hampsterdance.com")
  val uri2 = URI("http://zombo.com")
  val uri3 = URI("http://realultimatepower.net")
  val uri4 = URI("http://timecube.com")
  val uri5 = URI("http://ytmnd.com")
  val refByUri = URI("http://isRef.by")

  val schema = new Schema.Parser().parse(new FlatFileIO().readFileAsString("/avro/MAPRecord.avsc"))
  val sqlSchema = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
  val enrichedRecord = EnrichedRecordFixture.enrichedRecord

  val dcmiTypeCollection = enrichedRecord.sourceResource.collection
    .headOption.getOrElse(throw new RuntimeException("You cut off my head"))
  val emptyDcmiTypeCollection = DcmiTypeCollection()

  val edmTimeSpan = enrichedRecord.sourceResource.date
    .headOption.getOrElse(throw new RuntimeException("You cut off my head"))
  val emptyEdmTimeSpan = EdmTimeSpan()

  val dplaPlace = DplaPlace(
    name = Some("Boston"),
    city = Some("Boston"),
    county = Some("Suffolk County"),
    state = Some("Massachusetts"),
    country = Some("United States of America"),
    region = Some("North America"),
    coordinates = Some("42.358333,71.059722")
  )
  val emptyDplaPlace = DplaPlace()

  val skosConcept = SkosConcept(
    concept = Some("Food"),
    providedLabel = Some("Food label"),
    note = Some("Food notes"),
    scheme = Some(uri1),
    exactMatch = Seq(uri2, uri3),
    closeMatch = Seq(uri4, uri5)
  )
  val emtpySkosConcept = SkosConcept()

  val stringLiteralOrUri = Left("String")
  val uriLiteralOrUri = Right(uri1)

  val edmAgent = EdmAgent(
    uri = Some(uri1),
    name = Some("Michael Scott"),
    providedLabel = Some("Michael Scarn"),
    note = Some("C#"),
    scheme = Some(uri5),
    exactMatch = Seq(uri1, uri2),
    closeMatch = Seq(uri3, uri4)
  )
  val emptyEdmAgent = EdmAgent()

  val edmWebResource = EdmWebResource(
    uri = uri1,
    fileFormat = Seq("image/gif", "image/jpeg"),
    dcRights = Seq("free speech", "peaceful assembly"),
    edmRights = Some("trial by jury"),
    isReferencedBy = Some(refByUri)
  )

  val emptyEdmWebResource = EdmWebResource(uri = uri1)

  "A RowConverter" should "convert a DplaMapModel to a Row with a schema" in {
    val row = RowConverter.toRow(enrichedRecord, sqlSchema)
  }

  it should "convert a DcmiTypeCollection" in {
    val row = RowConverter.fromDcmiTypeCollection(dcmiTypeCollection)
    assert(row(0) === dcmiTypeCollection.title.orNull)
    assert(row(1) === dcmiTypeCollection.description.orNull)
  }

  it should "convert an empty DcmiTypeCollection" in {
    val row = RowConverter.fromDcmiTypeCollection(emptyDcmiTypeCollection)
    assert(row(0) === null)
    assert(row(1) === null)
  }

  it should "convert an EdmTimeSpan" in {
    val row = RowConverter.fromEdmTimeSpan(edmTimeSpan)
    assert(row(0) === edmTimeSpan.originalSourceDate.orNull)
    assert(row(1) === edmTimeSpan.prefLabel.orNull)
    assert(row(2) === edmTimeSpan.begin.orNull)
    assert(row(3) === edmTimeSpan.end.orNull)
  }

  it should "convert an empty EdmTimeSpan" in {
    val row = RowConverter.fromEdmTimeSpan(emptyEdmTimeSpan)
    assert(row(0) === null)
    assert(row(1) === null)
    assert(row(2) === null)
    assert(row(3) === null)
  }

  it should "convert a SkosConcept" in {
    val row = RowConverter.fromSkosConcept(skosConcept)
    assert(row(0) === skosConcept.concept.orNull)
    assert(row(1) === skosConcept.providedLabel.orNull)
    assert(row(2) === skosConcept.note.orNull)
    assert(row(3) === skosConcept.scheme.map(_.toString).orNull)
    assert(row(4) === skosConcept.exactMatch.map(_.toString))
    assert(row(5) === skosConcept.closeMatch.map(_.toString))
  }

  it should "convert an empty SkosConcpet" in {
    val row = RowConverter.fromSkosConcept(emtpySkosConcept)
    assert(row(0) === null)
    assert(row(1) === null)
    assert(row(2) === null)
    assert(row(3) === null)
    assert(row(4) === Seq())
    assert(row(5) === Seq())
  }

  it should "convert a DplaPlace" in {
    val row = RowConverter.fromDplaPlace(dplaPlace)
    assert(row(0) === dplaPlace.name.orNull)
    assert(row(1) === dplaPlace.city.orNull)
    assert(row(2) === dplaPlace.county.orNull)
    assert(row(3) === dplaPlace.region.orNull)
    assert(row(4) === dplaPlace.state.orNull)
    assert(row(5) === dplaPlace.country.orNull)
    assert(row(6) === dplaPlace.coordinates.orNull)
  }

  it should "convert an empty DplaPlace" in {
    val row = RowConverter.fromDplaPlace(emptyDplaPlace)
    assert(row(0) === null)
    assert(row(1) === null)
    assert(row(2) === null)
    assert(row(3) === null)
    assert(row(4) === null)
    assert(row(5) === null)
    assert(row(6) === null)
  }

  it should "convert a LiteralOrUri" in {
    val stringRow = RowConverter.fromLiteralOrUri(stringLiteralOrUri)
    assert(stringRow(0) === "String")
    assert(stringRow(1) === false)

    val uriRow = RowConverter.fromLiteralOrUri(uriLiteralOrUri)
    assert(uriRow(0) === uri1.toString)
    assert(uriRow(1) === true)
  }

  it should "convert an EdmAgent" in {
    val row = RowConverter.fromEdmAgent(edmAgent)
    assert(row(0) === edmAgent.uri.map(_.toString).orNull)
    assert(row(1) === edmAgent.name.orNull)
    assert(row(2) === edmAgent.providedLabel.orNull)
    assert(row(3) === edmAgent.note.orNull)
    assert(row(4) === edmAgent.scheme.map(_.toString).orNull)
    assert(row(5) === edmAgent.exactMatch.map(_.toString))
    assert(row(6) === edmAgent.closeMatch.map(_.toString))
  }

  it should "convert an empty EdmAgent" in {
    val row = RowConverter.fromEdmAgent(emptyEdmAgent)
    assert(row(0) === null)
    assert(row(1) === null)
    assert(row(2) === null)
    assert(row(3) === null)
    assert(row(4) === null)
    assert(row(5) === Seq())
    assert(row(6) === Seq())
  }

  it should "convert an EdmWebResource" in {
    val row = RowConverter.fromEdmWebResource(edmWebResource)
    assert(row(0) === edmWebResource.uri.toString)
    assert(row(1) === edmWebResource.fileFormat)
    assert(row(2) === edmWebResource.dcRights)
    assert(row(3) === edmWebResource.edmRights.orNull)
    assert(row(4) === edmWebResource.isReferencedBy.map(_.toString).orNull)
  }

  it should "convert an empty EdmWebResource" in {
    val row = RowConverter.fromEdmWebResource(emptyEdmWebResource)
    assert(row(0) === emptyEdmWebResource.uri.toString)
    assert(row(1) === Seq())
    assert(row(2) === Seq())
    assert(row(3) === null)
    assert(row(4) === emptyEdmWebResource.isReferencedBy.map(_.toString).orNull)
  }

  /*
  dplaUri: ExactlyOne[URI],
                           sourceResource: ExactlyOne[DplaSourceResource],
                           dataProvider: ExactlyOne[EdmAgent],
                           originalRecord: ExactlyOne[String],
                           hasView: ZeroToMany[EdmWebResource] = Seq(),
                           intermediateProvider: ZeroToOne[EdmAgent] = None,
                           isShownAt: ExactlyOne[EdmWebResource],
                           `object`: ZeroToOne[EdmWebResource] = None, // full size image
                           preview: ZeroToOne[EdmWebResource] = None, // thumbnail
                           provider: ExactlyOne[EdmAgent],
                           edmRights: ZeroToOne[URI] = None
   */

  it should "convert an OreAggregation" in {
    val oreAggregation = OreAggregation(
      dplaUri = uri3,
      sourceResource = DplaSourceResource(),
      dataProvider = edmAgent,
      originalRecord = "I'm very original",
      hasView = Seq(edmWebResource, edmWebResource),
      intermediateProvider = Some(edmAgent),
      isShownAt = edmWebResource,
      `object` = Some(edmWebResource),
      preview = Some(edmWebResource),
      provider = edmAgent,
      edmRights = Some(uri1),
      originalId = Some("original ID")
    )
    val row = RowConverter.toRow(oreAggregation, sparkSchema)
    assert(row(0) === oreAggregation.dplaUri.toString)
    assert(row(1) === RowConverter.fromSourceResource(oreAggregation.sourceResource))
    assert(row(2) === RowConverter.fromEdmAgent(oreAggregation.dataProvider))
    assert(row(3) === oreAggregation.originalRecord)
    assert(row(4) === oreAggregation.hasView.map(RowConverter.fromEdmWebResource))
    assert(row(5) === oreAggregation.intermediateProvider.map(RowConverter.fromEdmAgent).orNull)
    assert(row(6) === RowConverter.fromEdmWebResource(oreAggregation.isShownAt))
    assert(row(7) === oreAggregation.`object`.map(RowConverter.fromEdmWebResource).orNull)
    assert(row(8) === oreAggregation.preview.map(RowConverter.fromEdmWebResource).orNull)
    assert(row(9) === RowConverter.fromEdmAgent(oreAggregation.provider))
    assert(row(10) === oreAggregation.edmRights.map(_.toString).orNull)
    assert(row(13) === oreAggregation.originalId.orNull)
  }

  it should "convert a SourceResource" in {
    val sourceResource = DplaSourceResource(
      alternateTitle = Seq("fee", "fie", "fo"),
      collection = Seq(dcmiTypeCollection, dcmiTypeCollection, dcmiTypeCollection),
      contributor = Seq(edmAgent, edmAgent),
      creator = Seq(edmAgent, edmAgent, edmAgent),
      date = Seq(edmTimeSpan, edmTimeSpan, edmTimeSpan),
      description = Seq("this", "is", "description"),
      extent = Seq("very", "extensive"),
      format = Seq("hfs+", "fat32"),
      genre = Seq(skosConcept, skosConcept, skosConcept),
      identifier = Seq("can", "I", "see", "some", "id"),
      language = Seq(skosConcept, skosConcept, skosConcept),
      place = Seq(dplaPlace, dplaPlace, dplaPlace),
      publisher = Seq(edmAgent, edmAgent, edmAgent),
      relation = Seq(Right(uri1), Left("foobar"), Right(uri2), Left("snozzbuzz")),
      replacedBy = Seq("someone's", "taken", "my", "place"),
      replaces = Seq("we're", "the", "replacmements"),
      rights = Seq("some", "rights"),
      rightsHolder = Seq(edmAgent, edmAgent),
      subject = Seq(skosConcept, skosConcept),
      temporal = Seq(edmTimeSpan, edmTimeSpan),
      title = Seq("some", "title"),
      `type` = Seq("rock", "magic", "grass")
    )
    val row = RowConverter.fromSourceResource(sourceResource)
    assert(row(0) === sourceResource.alternateTitle)
    assert(row(1) === sourceResource.collection.map(RowConverter.fromDcmiTypeCollection))
    assert(row(2) === sourceResource.contributor.map(RowConverter.fromEdmAgent))
    assert(row(3) === sourceResource.creator.map(RowConverter.fromEdmAgent))
    assert(row(4) === sourceResource.date.map(RowConverter.fromEdmTimeSpan))
    assert(row(5) === sourceResource.description)
    assert(row(6) === sourceResource.extent)
    assert(row(7) === sourceResource.format)
    assert(row(8) === sourceResource.genre.map(RowConverter.fromSkosConcept))
    assert(row(9) === sourceResource.identifier)
    assert(row(10) === sourceResource.language.map(RowConverter.fromSkosConcept))
    assert(row(11) === sourceResource.place.map(RowConverter.fromDplaPlace))
    assert(row(12) === sourceResource.publisher.map(RowConverter.fromEdmAgent))
    assert(row(13) === sourceResource.relation.map(RowConverter.fromLiteralOrUri))
    assert(row(14) === sourceResource.replacedBy)
    assert(row(15) === sourceResource.replaces)
    assert(row(16) === sourceResource.rights)
    assert(row(17) === sourceResource.rightsHolder.map(RowConverter.fromEdmAgent))
    assert(row(18) === sourceResource.subject.map(RowConverter.fromSkosConcept))
    assert(row(19) === sourceResource.temporal.map(RowConverter.fromEdmTimeSpan))
    assert(row(20) === sourceResource.title)
    assert(row(21) === sourceResource.`type`)

  }
}