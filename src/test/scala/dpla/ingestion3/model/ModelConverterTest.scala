package dpla.ingestion3.model

import com.databricks.spark.avro.SchemaConverters
import dpla.ingestion3.data.EnrichedRecordFixture
import dpla.ingestion3.utils.FlatFileIO
import org.apache.avro.Schema
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfter, FlatSpec}


class ModelConverterTest extends FlatSpec with BeforeAndAfter {

  val schema = new Schema.Parser().parse(new FlatFileIO().readFileAsString("/avro/MAPRecord.avsc"))
  val sqlSchema = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
  val enrichedRecord = EnrichedRecordFixture.enrichedRecord

  val urlString1 = "http://example.com"
  val urlString2 = "http://google.com"
  val urlString3 = "http://yahoo.com"
  val urlisRefBy = "http://isRefd.by/"

  val testEdmAgent = Row(
    urlString1,
    "Sam",
    "Sam Example",
    "F#",
    urlString2,
    Seq(urlString3, urlString2),
    Seq(urlString3, urlString1)
  )

  val testIngestMessage = Row("msg", "ERROR", "123", "type", "StillImage", "images")

  val testEdmWebResource = Row(urlString1, Seq("foo"), Seq("bar"), "baz", urlisRefBy)

  val testEdmTimeSpan = Row("2012", "2013", "2014", "2015")

  val testDcmiTypeCollection = Row("Foo", "Bar")

  val testSkosConcept = Row("foo", "bar", "baz", urlString1, Seq(urlString2, urlString3), Seq(urlString1))

  val testDplaPlace = Row("foo", "bar", "baz", "buzz", "Booga", "Wooga", "Oooga")

  val literal = Row("I'm very literal", false)
  val uri = Row(urlString1, true)

  val stringSeq = Seq("booga", "wooga", "oooga")
  val testSourceResource = Row(
    stringSeq,                                          //alternateTitle
    Seq(testDcmiTypeCollection, testDcmiTypeCollection),//collection
    Seq(testEdmAgent, testEdmAgent),                    //contributor
    Seq(testEdmAgent, testEdmAgent),                    //creator
    Seq(testEdmTimeSpan, testEdmTimeSpan),              //date
    stringSeq,                                          //description
    stringSeq,                                          //extent
    stringSeq,                                          //format
    Seq(testSkosConcept, testSkosConcept),              //genre
    stringSeq,                                          //identifier
    Seq(testSkosConcept, testSkosConcept),              //language
    Seq(testDplaPlace, testDplaPlace),                  //place,
    Seq(testEdmAgent, testEdmAgent),                    //publisher
    Seq(literal, literal, uri),                         //relation
    stringSeq,                                          //replacedBy
    stringSeq,                                          //replaces
    stringSeq,                                          //rights
    Seq(testEdmAgent, testEdmAgent),                    //rightsHolder
    Seq(testSkosConcept, testSkosConcept),              //subject
    Seq(testEdmTimeSpan, testEdmTimeSpan),              //temporal
    stringSeq,                                          //title
    stringSeq                                           //type
  )


  "A ModelConverter" should "work with RowConverter and round trip a DplaMapModel" in {
    val row = RowConverter.toRow(enrichedRecord, sqlSchema)
    val roundTripRecord = ModelConverter.toModel(row)
  }

  it should "extract a sequence of URIs from a Row" in {
    val data = Row(Seq(urlString1, urlString2, urlString3))
    val output = ModelConverter.uriSeq(data, 0)
    assert(output === Seq(urlString1, urlString2, urlString3).map(new URI(_)))
  }

  it should "extract no URIs from an empty Row" in {
    val output = ModelConverter.uriSeq(Row(Seq()), 0)
    assert(output === Seq())
  }

  it should "extract a sequence of strings from a Row" in {
    val seq = Seq("eenie", "miney", "moe")
    val data = Row(seq)
    val output = ModelConverter.stringSeq(data, 0)
    assert(output === seq)
  }

  it should "extract no strings from an empty Row" in {
    val output = ModelConverter.stringSeq(Row(Seq()), 0)
    assert(output === Seq())
  }

  it should "extract an Option[String] from a Row" in {
    val output = ModelConverter.optionalString(Row("foo"), 0)
    assert(output === Some("foo"))
  }

  it should "extract None when no string is present" in {
    val output = ModelConverter.optionalString(Row(null), 0)
    assert(output === None)
  }

  it should "extract a string when one is expected to be present" in {
    val output = ModelConverter.requiredString(Row("foo"), 0)
    assert(output === "foo")
  }

  it should "throw an exception when a required string isn't there" in {
    assertThrows[RuntimeException](ModelConverter.requiredString(Row(null), 0))
  }

  it should "extract an Option[URI] from a Row" in {
    val output = ModelConverter.optionalUri(Row(urlString1), 0)
    assert(output === Some(new URI(urlString1)))
  }

  it should "extract None when no uri is present" in {
    val output = ModelConverter.optionalUri(Row(null), 0)
    assert(output === None)
  }

  it should "extract a URI when one is expected to be present" in {
    val output = ModelConverter.requiredUri(Row(urlString3), 0)
    assert(output === new URI(urlString3))
  }

  it should "throw an exception when a required uri isn't there" in {
    assertThrows[RuntimeException](ModelConverter.requiredUri(Row(null), 0))
  }

  it should "extract Rows from a subfield" in {
    val output = ModelConverter.toRows(Row(Seq(Seq("a", "b", "c"), Seq("1", "2", "3"))), 0)
    assert(output.nonEmpty)
    assert(output.size === 2)
    assert(output.headOption.getOrElse(Seq()) === Seq("a", "b", "c"))
    assert(output(1) === Seq("1", "2", "3"))
  }

  it should "perform transformations on multivalued fields" in {
    val testData = Row(Seq(Row(1), Row(2)))
    val function = (row: Row) => row.get(0).toString
    val output = ModelConverter.toMulti(testData, 0, function)
    assert(output.nonEmpty)
    assert(output.size === 2)
    assert(output.headOption.getOrElse("") === "1")
    assert(output(1) === "2")
  }

  it should "convertRowsToEdmAgent" in {
    val edmAgent = ModelConverter.toEdmAgent(testEdmAgent)
    val uri = edmAgent.uri.orNull
    assert(edmAgent.uri.map(_.value).orNull === testEdmAgent(0))
    assert(edmAgent.name.orNull === testEdmAgent(1))
    assert(edmAgent.providedLabel.orNull === testEdmAgent(2))
    assert(edmAgent.note.orNull === testEdmAgent(3))
    assert(edmAgent.scheme.map(_.value).orNull === testEdmAgent(4))
    assert(edmAgent.exactMatch.map(_.value) === testEdmAgent.getSeq[String](5))
    assert(edmAgent.closeMatch.map(_.value) === testEdmAgent.getSeq[String](6))
  }

  it should "handle LiteralOrUri" in {
    val literalOrUri1 = ModelConverter.toLiteralOrUri(literal)
    assert(literalOrUri1.isLeft)
    assert(literalOrUri1.left.getOrElse("") === "I'm very literal")

    val literalOrUri2 = ModelConverter.toLiteralOrUri(uri)
    assert(literalOrUri2.isRight)
    assert(literalOrUri2.right.getOrElse(new URI("http://example.com")) === new URI(urlString1))
  }

  it should "handle optional EdmWebResources" in {
    ModelConverter.toOptionEdmWebResource(testEdmWebResource) match {
      case Some(edmWebResource: EdmWebResource) => Unit
      case None => fail("Got a none back for something that should be a Some")
    }

    assert(ModelConverter.toOptionEdmWebResource(null) === None)
  }

  it should "convert DcmiTypeCollection" in {
    val testResult1 = ModelConverter.toDcmiTypeCollection(testDcmiTypeCollection)
    val testResult2 = ModelConverter.toDcmiTypeCollection(Row(null, null))

    assert(testResult1.title === Some("Foo"))
    assert(testResult1.description === Some("Bar"))
    assert(testResult2.title === None)
    assert(testResult2.description === None)
  }

  it should "convert EdmTimeSpan" in {
    val testResult1 = ModelConverter.toEdmTimeSpan(testEdmTimeSpan)
    val testResult2 = ModelConverter.toEdmTimeSpan(Row(null, null, null, null))

    assert(testResult1.originalSourceDate === Some("2012"))
    assert(testResult1.prefLabel === Some("2013"))
    assert(testResult1.begin === Some("2014"))
    assert(testResult1.end === Some("2015"))

    assert(testResult2.originalSourceDate === None)
    assert(testResult2.prefLabel === None)
    assert(testResult2.begin === None)
    assert(testResult2.end === None)
  }

  it should "convert SkosConcept" in {
    val testResult1 = ModelConverter.toSkosConcept(testSkosConcept)
    val testResult2 = ModelConverter.toSkosConcept(Row(null, null, null, null, Seq(), Seq()))

    assert(testResult1.concept === Some("foo"))
    assert(testResult1.providedLabel === Some("bar"))
    assert(testResult1.note === Some("baz"))
    assert(testResult1.scheme === Some(new URI(urlString1)))
    assert(testResult1.exactMatch === Seq(new URI(urlString2), new URI(urlString3)))
    assert(testResult1.closeMatch === Seq(new URI(urlString1)))

    assert(testResult2.concept === None)
    assert(testResult2.providedLabel === None)
    assert(testResult2.note === None)
    assert(testResult2.scheme === None)
    assert(testResult2.exactMatch === Seq())
    assert(testResult2.closeMatch === Seq())
  }

  it should "convert DplaPlace" in {
    val testResult1 = ModelConverter.toDplaPlace(testDplaPlace)
    val testResult2 = ModelConverter.toDplaPlace(Row(null, null, null, null, null, null, null))

    assert(testResult1.name === Some("foo"))
    assert(testResult1.city === Some("bar"))
    assert(testResult1.county === Some("baz"))
    assert(testResult1.region === Some("buzz"))
    assert(testResult1.state === Some("Booga"))
    assert(testResult1.country === Some("Wooga"))
    assert(testResult1.coordinates === Some("Oooga"))

    assert(testResult2.name === None)
    assert(testResult2.city === None)
    assert(testResult2.county === None)
    assert(testResult2.region === None)
    assert(testResult2.state === None)
    assert(testResult2.country === None)
    assert(testResult2.coordinates === None)
  }

  it should "convert an OreAggregation" in {
    val testResult1 = ModelConverter.toModel(
      Row(
        urlString1,
        testSourceResource,
        testEdmAgent,
        "an original record",
        Seq(testEdmWebResource, testEdmWebResource),
        testEdmAgent,
        testEdmWebResource,
        testEdmWebResource,
        testEdmWebResource,
        testEdmAgent,
        urlString1,
        """"{"field": "value"}""",
        Seq(testIngestMessage)
      )
    )

    val edmAgent = ModelConverter.toEdmAgent(testEdmAgent)
    val edmWebResource = ModelConverter.toEdmWebResource(testEdmWebResource)

    assert(testResult1.dplaUri === new URI(urlString1))
    assert(testResult1.dataProvider === edmAgent)
    assert(testResult1.originalRecord === "an original record" )
    assert(testResult1.hasView === Seq(edmWebResource, edmWebResource))
    assert(testResult1.intermediateProvider === Some(edmAgent))
    assert(testResult1.`object` === Some(edmWebResource))
    assert(testResult1.preview === Some(edmWebResource))
    assert(testResult1.provider === edmAgent)
    assert(testResult1.edmRights === Some(new URI(urlString1)))
  }

  it should "convert an EdmWebResource" in {
    val testResult = ModelConverter.toEdmWebResource(testEdmWebResource)
    assert(testResult.uri === new URI(urlString1))
    assert(testResult.fileFormat === Seq("foo"))
    assert(testResult.dcRights === Seq("bar"))
    assert(testResult.edmRights === Some("baz"))
  }

  it should "convert a SourceResource" in {

    val output = ModelConverter.toSourceResource(testSourceResource)
    assert(ModelConverter.stringSeq(testSourceResource, 0) === output.alternateTitle)
    assert(testSourceResource.getSeq[Row](1).map(ModelConverter.toDcmiTypeCollection) === output.collection)
    assert(testSourceResource.getSeq[Row](2).map(ModelConverter.toEdmAgent) === output.contributor)
    assert(testSourceResource.getSeq[Row](3).map(ModelConverter.toEdmAgent) === output.creator)
    assert(testSourceResource.getSeq[Row](4).map(ModelConverter.toEdmTimeSpan) === output.date)
    assert(ModelConverter.stringSeq(testSourceResource, 5) === output.description)
    assert(ModelConverter.stringSeq(testSourceResource, 6) === output.extent)
    assert(ModelConverter.stringSeq(testSourceResource, 7) === output.format)
    assert(testSourceResource.getSeq[Row](8).map(ModelConverter.toSkosConcept) === output.genre)
    assert(ModelConverter.stringSeq(testSourceResource, 9) === output.identifier)
    assert(testSourceResource.getSeq[Row](10).map(ModelConverter.toSkosConcept) === output.language)
    assert(testSourceResource.getSeq[Row](11).map(ModelConverter.toDplaPlace) === output.place)
    assert(testSourceResource.getSeq[Row](12).map(ModelConverter.toEdmAgent) === output.publisher)
    assert(testSourceResource.getSeq[Row](13).map(ModelConverter.toLiteralOrUri) === output.relation)
    assert(ModelConverter.stringSeq(testSourceResource, 14) === output.replacedBy)
    assert(ModelConverter.stringSeq(testSourceResource, 15) === output.replaces)
    assert(ModelConverter.stringSeq(testSourceResource, 16) === output.rights)
    assert(testSourceResource.getSeq[Row](17).map(ModelConverter.toEdmAgent) === output.rightsHolder)
    assert(testSourceResource.getSeq[Row](18).map(ModelConverter.toSkosConcept) === output.subject)
    assert(testSourceResource.getSeq[Row](19).map(ModelConverter.toEdmTimeSpan) === output.temporal)
    assert(ModelConverter.stringSeq(testSourceResource, 20) === output.title)
    assert(ModelConverter.stringSeq(testSourceResource, 21) === output.`type`)
  }
}
