package dpla.ingestion3.mappers.providers

import dpla.ingestion3.mappers.utils.Document
import dpla.ingestion3.model.{DcmiTypeCollection, EdmAgent, EdmTimeSpan, EdmWebResource, SkosConcept, URI}
import org.json4s.{JString, JValue}
import org.json4s.JsonAST.JArray
import org.scalatest.{FlatSpec, FunSuite}
import com.github.tototoshi.csv._

class KenningMappingTest extends FlatSpec {

  private val rowString = "P1070996.jpeg,Starting the hike at the Heart Lake trailhead," +
    "\"Kenning Arlitsch, Robert Patrick, Douglas Wollant, and Bob Hietala start " +
    "their trip at the trailhead near Lewis Lake. The sign indicates 7.4 miles " +
    "to Heart Lake.\",2019-09-09,Yellowstone National Park--Description and " +
    "Travel; Backpacking; Walking -- Yellowstone National Park,This work is " +
    "licensed under a Creative Commons Attribution-NonCommercial-ShareAlike 4.0 " +
    "International License,Kenning Arlitsch"

  private val doc =
    Document[JValue](
      JArray(
        CSVParser
          .parse(rowString, '\\', ',', '"')
          .getOrElse(List())
          .map(JString(_))
      )
    )

  val km = new KenningMapping()

  "A KenningMapping" should "map a csv row to a title" in
    assert(km.title(doc) === Seq("Starting the hike at the Heart Lake trailhead"))

  it should "map original id" in
    assert(km.originalId(doc) === Some("P1070996.jpeg"))

  it should "map a DPLA URI" in
    assert(km.dplaUri(doc) === Some(URI("http://dp.la/api/items/d8e6924589c1977ea2318451a94f3ed0")))

  it should "map sidecar" in
    assert(km.sidecar(doc) !== null)

  it should "map dataProvider" in
    assert(km.dataProvider(doc) === Seq(EdmAgent(name = Some("Kenning Arlitsch Personal Photographs Collection"))))

  it should "map originalRecord" in
    assert(km.originalRecord(doc) !== null)

  it should "map a provider" in
    assert(km.provider(doc) === EdmAgent(name = Some("Montana State University"), uri = Some(URI("http://dp.la/api/contributor/msu"))))

  it should "map a collection" in
    assert(km.collection(doc) === Seq(DcmiTypeCollection(
      title = Some("Kenning Arlitsch Personal Photographs Collection")
    )))

  it should "map isShownAt" in
    assert(km.isShownAt(doc) === Seq(EdmWebResource(uri = URI("http://google.com"))))

  it should "map contributor" in
    assert(km.contributor(doc) === Seq(EdmAgent(name = Some("Kenning Arlitsch"))))

  it should "map date" in
    assert(km.date(doc) === Seq(EdmTimeSpan(originalSourceDate = Some("2019-09-09"))))

  it should "map description" in
    assert(km.description(doc) === Seq("Kenning Arlitsch, Robert Patrick, Douglas Wollant, and Bob Hietala start their trip at the trailhead near Lewis Lake. The sign indicates 7.4 miles to Heart Lake."))

  it should "map format" in
    assert(km.format(doc) === Seq("Photograph"))

  it should "map subject" in
    assert(km.subject(doc) === Seq(
      SkosConcept(providedLabel = Some("Yellowstone National Park--Description and Travel")),
      SkosConcept(providedLabel = Some("Backpacking")),
      SkosConcept(providedLabel = Some("Walking -- Yellowstone National Park")))
    )

  it should "map type" in
    assert(km.`type`(doc) === Seq("Image"))

}
