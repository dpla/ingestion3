package dpla.ingestion3.mappers.json

import org.scalatest._
import org.json4s.JsonAST._

class JsonExtractionUtilsTest extends FlatSpec with JsonExtractionUtils {

  "A JsonExtractionUtils" should "extract strings from a field named by a string" in {
    val jvalue = JObject(JField("foo", JString("bar")))
    assert(extractString("foo")(jvalue) === Some("bar"))
  }

  it should "return a None when there is no field named by a string" in {
    val jvalue = JObject(JField("foo", JString("bar")))
    assert(extractString("bar")(jvalue) === None)
  }

  it should "extract multiple strings from a field named by a string" in {
    val jvalue = JObject(JField("foo", JArray(List(JString("foo"), JString("bar")))))
    val strings = extractStrings("foo")(jvalue)

    assert(strings.contains("foo"))
    assert(strings.contains("bar"))
  }

  it should "extract an empty Seq if there are no values available in a field named by a string" in {
    val jvalue = JObject(JField("foo", JString("bar")))
    val strings = extractStrings("blah")(jvalue)

    assert(strings.length === 0)
  }

  it should "extract strings from a JList" in {
    val jvalue = JArray(List(JString("foo"), JString("bar"), JString("baz"), JString("buzz")))
    val strings = extractStrings(jvalue)

    assert(strings.length === 4)
    assert(strings.contains("foo"))
    assert(strings.contains("bar"))
    assert(strings.contains("baz"))
    assert(strings.contains("buzz"))
  }

  it should "extract key values from a JObject" in {
    val jobject = JObject(
      JField("1", JString("foo")),
      JField("2", JString("bar")),
      JField("3", JString("baz"))
    )

    val strings = extractStrings(jobject)
    assert(strings.length === 3)
    assert(strings.contains("foo"))
    assert(strings.contains("bar"))
    assert(strings.contains("baz"))
  }

  it should
    "extract a single valued Seq from anything that's not a JObject or a JList but can be stringified" in {
    val testData = Seq(
      (JString("foo"), "foo"),
      (JInt(1), "1"),
      (JDecimal(1.1), "1.1"),
      (JDouble(1.1), "1.1"),
      (JBool(true), "true")
    )

    for (pair <- testData) pair match {
      case (input, expected) => {
        val result = extractStrings(input)
        assert(result.length === 1)
        assert(result.contains(expected))
      }
    }
  }

  it should "not extract anything for single valued nodes that can't be stringified" in {
    val testData = Seq(
      JNothing,
      JNull
    )

    for (value <- testData) {
      val result = extractStrings(value)
      assert(result === Seq())
    }
  }
}
