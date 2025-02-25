package dpla.ingestion3.entries.utils

import dpla.ingestion3.utils.HttpUtils
import org.json4s.{JArray, JField, JNull}
import org.json4s.JsonAST.{JObject, JString}
import org.json4s.jackson.JsonMethods

import java.io.File
import java.net.{URL, URLEncoder}

object UpdateInstitutions extends App {

  val apiKey = args(0)
  val institutionsFilePath = "src/main/resources/wiki/institutions_v2.json"
  val institutionsJson = JsonMethods.parse(new File(institutionsFilePath))

  val hubsResponse = HttpUtils.makeGetRequest(
    new URL("https://api.dp.la/items?facets=provider.name&page_size=0&facet_size=2000"),
    Some(Map(("Authorization", apiKey)))
  )

  val hubsJson = JsonMethods.parse(hubsResponse)
  val hubsNames =  for {
    JString(term) <- hubsJson \ "facets" \ "provider.name" \ "terms" \ "term"
  } yield term

  if (hubsNames.length == 2000) {
    println("Warning: 2000 hubs found, some may be missing")
  }

  var count = 0
  for (hubName <- hubsNames) {
    val hubData = institutionsJson \ hubName match {
      case existing: JObject => existing
      case _ =>
        val hubData = JObject(
          List(
            JField("Wikidata", JNull),
            JField("institutions", JArray(Nil))
          )
        )
        hubsJson.merge(JObject(List(JField(hubName, hubData))))
        hubData
    }
    val escapedName = URLEncoder.encode(hubName.replaceAll("/", " "), "UTF-8")
    val contributorResponse = HttpUtils.makeGetRequest(
        new URL(s"https://api.dp.la/items?facets=dataProvider.name&provider.name=$escapedName&page_size=0&facet_size=2000"),
        Some(Map(("Authorization", apiKey)))
    )
    val contributorJson = JsonMethods.parse(contributorResponse)
    val contributorNames = for {
      JString(term) <- contributorJson \ "facets" \ "dataProvider.name" \ "terms" \ "term"
    } yield term

    if (contributorNames.length == 2000) {
      println(s"Warning: 2000 contributors found for $hubName, some may be missing")
    }

    contributorNames.foreach(name => println(f"$hubName -> $name"))
    count = count + contributorNames.length
  }
  println(f"$count contributors found")

//  val results = HttpUtils.makeGetRequest(
//    new URL("https://api.dp.la/items&facets=dataProvider"),
//    Some(Map(("Authorization", apiKey))))


}
