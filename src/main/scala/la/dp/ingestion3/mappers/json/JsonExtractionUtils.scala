package la.dp.ingestion3.mappers.json


import org.json4s.JValue
import org.json4s.JsonAST._

/**
  * Utils to help extract data from JSON documents.
  *
  */
trait JsonExtractionUtils {

  /**
    * Pulls a single string from the implicit json data.
    *
    * @param fieldName Name of field to extract string from.
    * @param json      JSON document or subdocument
    * @return Some(string) if one could be found. Will fail
    *         on non-primitive values like arrays or objects.
    */
  def extractString(fieldName: String)(implicit json: JValue): Option[String]
  = extractString(json \ fieldName)

  /**
    * Pulls a Seq[String] of values from the implicit json daa
    *
    * @param fieldName Name of field to extract string from.
    * @param json      JSON document or subdocument
    * @return A Seq[String].
    *
    *         If the field is an array, the Seq will contain the array contents,
    *         if they were themselves primitive values.
    *
    *         If the field is an object, the Seq will contain the object field
    *         values, if those were primitive values.
    *
    *         If the field is a primitive, a Seq with a single member is returned.
    *
    *         Otherwise, an empty Seq is returned.
    */
  def extractStrings(fieldName: String)(implicit json: JValue): Seq[String]
  = extractStrings(json \ fieldName)

  /**
    * @see definition of extractStrings(fieldName: String), save for this version
    *      can be called with a parameter that generates a JValue, such as json4s'
    *      path-walking syntax: jsonTree \ "someChild" \\ "someDecendant"
    *
    */
  def extractStrings(jValue: JValue): Seq[String] = jValue match {
    case JArray(array) => array.flatMap(entry => extractString(entry))
    case JObject(fields) => fields.flatMap(field => extractString(field._2))
    case _ => extractString(jValue) match {
      case Some(stringValue) => Seq(stringValue)
      case None => Seq()
    }
  }

  /**
    * @see definition of extractString(fieldName: String), save for this version
    *      can be called with a parameter that generates a JValue, such as json4s'
    *      path-walking syntax: jsonTree \ "someChild" \\ "someDecendant"
    *
    */
  def extractString(jValue: JValue): Option[String] = jValue match {
    case JBool(bool) => Some(bool.toString)
    case JDecimal(decimal) => Some(decimal.toString())
    case JDouble(double) => Some(double.toString())
    case JInt(int) => Some(int.toString())
    case JString(string) => Some(string)
    case _ => None
  }

}