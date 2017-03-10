package dpla.ingestion3.mappers.json

import org.scalatest._
import org.json4s.JsonAST._

class JsonExtractionUtilsTest extends FlatSpec with Matchers with BeforeAndAfter with JsonExtractionUtils {

  "A JsonExtractionUtils" should "extract strings from a field named by a string" in {
    val jvalue = JObject(JField("foo", JString("bar")))
    extractString("foo")(jvalue) should be(Some("bar"))
  }

  "it" should "return a None when there is no field named by a string" in {
    val jvalue = JObject(JField("foo", JString("bar")))
    extractString("bar")(jvalue) should be(None)
  }

  "it" should "extract multiple strings from a field named by a string" in {
    val jvalue = JObject(JField("foo", JArray(List(JString("foo"), JString("bar")))))
    val strings = extractStrings("foo")(jvalue)

    strings should contain("foo")
    strings should contain("bar")
  }

  "it" should "extract an empty Seq if there are no values available in a field named by a string" in {
    val jvalue = JObject(JField("foo", JString("bar")))
    val strings = extractStrings("blah")(jvalue)

    strings should have length 0
  }

  "it" should "extract strings from a JList" in {
    val jvalue = JArray(List(JString("foo"), JString("bar"), JString("baz"), JString("buzz")))
    val strings = extractStrings(jvalue)

    strings should have length 4
    strings should contain("foo")
    strings should contain("bar")
    strings should contain("baz")
    strings should contain("buzz")
  }

  "it" should "extract key values from a JObject" in {
    val jobject = JObject(
      JField("1", JString("foo")),
      JField("2", JString("bar")),
      JField("3", JString("baz"))
    )

    val strings = extractStrings(jobject)
    strings should have length 3
    strings should contain("foo")
    strings should contain("bar")
    strings should contain("baz")
  }

  "it" should
    "extract a single valued Seq from anything that's not a JObject or a JList but can be stringified" in {
    val testData = Seq(
      (JString("foo"), "foo"),
      (JInt(1), "1"),
      (JDecimal(1.1), "1.1"),
      (JDouble(1.1), "1.1"),
      (JBool(true), "true")
    )

    for (pair <- testData) {
      val result = extractStrings(pair._1)
      result should have length 1
      result should contain (pair._2)
    }
  }

  "it" should "not extract anything for single valued nodes that can't be stringified" in {
    val testData = Seq(
      JNothing,
      JNull
    )

    for (value <- testData) {
      val result = extractStrings(value)
      result should be (Seq())
    }
  }
}
