package dpla.ingestion3.entries.utils

import org.json4s.JsonAST.{JObject, JString}
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.prefs.EmptyValueStrategy

import java.io.File
import java.net.URI
import java.net.http.HttpClient.Redirect
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.net.http.{HttpClient, HttpRequest}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Duration



object UpdateInstitutions {

  implicit val formats: Formats = DefaultFormats.withEmptyValueStrategy(
    EmptyValueStrategy.preserve
  )

  private type HubTuple = (String, Hub)
  private type ContributingInstitutionTuple = (String, ContributingInstitution)

  implicit def map2jvalueContributingInstitution(m: Map[String, ContributingInstitution])(implicit ev: ContributingInstitution => JValue): JObject =
    JObject(m.toList.sortWith( (a: ContributingInstitutionTuple, b: ContributingInstitutionTuple) => {a._1.compareTo(b._1) < 0}).map { case (k, v) => JField(k, ev(v)) })

  implicit def contributingInstitutionToJValue(contributingInstitution: ContributingInstitution): JValue = {
    ("Wikidata" -> contributingInstitution.Wikidata) ~
      ("upload" -> contributingInstitution.upload)
  }

  implicit def hubToJValue(hub: Hub): JValue = {
    ("Wikidata" -> hub.Wikidata) ~
      ("institutions" -> hub.institutions) ~
      ("upload" -> hub.upload)
  }

  implicit def map2jvalueHub(m: Map[String, Hub])(implicit ev: Hub => JValue): JObject =
    JObject(m.toList.sortWith( (a: HubTuple, b: HubTuple) => {a._1.compareTo( b._1) < 0}).map { case (k, v) => JField(k, ev(v)) })


  case class Hub(
                  Wikidata: Option[String] = Some(""),
                  institutions: Map[String, ContributingInstitution] = Map(),
                  upload: Option[Boolean] = Some(false)
                )


  case class ContributingInstitution(
      Wikidata: Option[String] = Some(""),
      upload: Option[Boolean] = Some(false)
  )

  private val ELASTICSEARCH_ENDPOINT =
    "http://search.internal.dp.la:9200/dpla_alias/_search"

  private val MAX_FACET_BUCKETS = 5000


  private val client = HttpClient
    .newBuilder()
    .followRedirects(Redirect.NORMAL)
    .connectTimeout(Duration.ofSeconds(20))
    .build()

  def getContributorNamesQuery(hubName: String): String = {
    val query = Map(
      "from" -> 0,
      "size" -> 0,
      "_source" -> Seq("*"),
      "query" -> Map(
        "bool" -> Map(
          "must" -> List(
            Map(
              "query_string" -> Map(
                "default_operator" -> "AND",
                "fields" -> List("provider.name"),
                "lenient" -> true,
                "query" -> hubName.replaceAll("/", "\\\\/")
              )
            )
          )
        )
      ),
      "aggs" -> Map(
        "dataProvider.name" -> Map(
          "terms" -> Map(
            "field" -> "dataProvider.name.not_analyzed",
            "size" -> MAX_FACET_BUCKETS
          )
        )
      )
    )
    Serialization.write(query)
  }

  private def execute(request: HttpRequest): String = {
    val response = client.send(request, BodyHandlers.ofString())
    if (response.statusCode() == 200) {
      response.body()
    } else {
      val msg = s"Unsuccessful request: ${request.uri().toString}\n" +
        s"Code: ${response.statusCode()}\n" +
        s"Message: ${response.body()}\n"
      throw new RuntimeException(msg)
    }
  }

  def getHubNamesQuery: String = {
    val query = Map(
      "from" -> 0,
      "size" -> 0,
      "_source" -> Seq("*"),
      "aggs" -> Map(
        "provider.name" -> Map(
          "terms" -> Map(
            "field" -> "provider.name.not_analyzed",
            "size" -> MAX_FACET_BUCKETS
          )
        )
      )
    )
    Serialization.write(query)
  }

  def buildPostRequest(query: String): HttpRequest = HttpRequest
    .newBuilder()
    .method("POST", BodyPublishers.ofString(query))
    .header("Content-Type", "application/json")
    .uri(new URI(ELASTICSEARCH_ENDPOINT))
    .build()

  def getHubNames: Seq[String] = {

    val query = getHubNamesQuery
    val request = buildPostRequest(query)
    val response = execute(request)
    val hubsJson = parse(response)

    val hubsNames =
      for (
        JString(term) <-
          hubsJson \ "aggregations" \ "provider.name" \ "buckets" \ "key"
      ) yield term

    if (hubsNames.length >= MAX_FACET_BUCKETS - 1)
      throw new RuntimeException(
        f"Warning: ${hubsNames.length} hubs found, some may be missing!"
      )

    hubsNames
  }

  def getContributorNames(hubName: String): Seq[String] = {
    val query = getContributorNamesQuery(hubName)
    val request = buildPostRequest(query)
    val response = execute(request)
    val contributorJson = parse(response)
    val contributorNames = for {
      JString(term) <-
        contributorJson \ "aggregations" \ "dataProvider.name" \ "buckets" \ "key"
    } yield term

    if (contributorNames.length >= MAX_FACET_BUCKETS - 1) {
      throw new RuntimeException(
        s"Warning: ${contributorNames.length} contributors found for $hubName, some may be missing!"
      )
    }

    contributorNames
  }

  def main(args: Array[String]): Unit = {
    val institutionsJson = parse(
      new File("src/main/resources/wiki/institutions_v2.json")
    )
    val institutionsData = (institutionsJson match {
        case JObject(values) =>
          values.map(value => value._1 -> value._2.extract[Hub])
        case _ =>
          throw new RuntimeException(
            "Can't understand existing institutions file."
          )
      }).toMap

    val hubNames = getHubNames

    val newHubs = hubNames
      .map(hubName => {
        val existingHub = institutionsData.getOrElse(hubName, Hub())
        hubName -> updatedHub(hubName, existingHub)
      })
      .toMap

    val withDroppedHubs = newHubs ++ institutionsData.keys.flatMap(hubName =>
      if (!newHubs.contains(hubName)) Some(hubName -> institutionsData(hubName))
      else None
    )

    // no backsliding
    for (hubName <- institutionsData.keys) {
      assert(
        withDroppedHubs.contains(hubName),
        f"Missing hub: $hubName"
      )
      val oldHub = institutionsData(hubName)
      val newHub = withDroppedHubs(hubName)
      assert(
        oldHub.Wikidata == newHub.Wikidata,
        f"Wikidata mismatch for hub: $hubName"
      )
      for (institutionName <- oldHub.institutions.keys) {
        assert(
          newHub.institutions.contains(institutionName),
          f"Missing institution: $institutionName in hub: $hubName"
        )
        val oldContributingInstitution = oldHub.institutions(institutionName)
        val newContributingInstitution = newHub.institutions(institutionName)
        assert(
          oldContributingInstitution.upload == newContributingInstitution.upload,
          f"Upload flag mismatch for institution: $institutionName in hub: $hubName"
        )
        assert(
          oldContributingInstitution.Wikidata == newContributingInstitution.Wikidata,
          f"Wikidata mismatch for institution: $institutionName in hub: $hubName"
        )
      }
    }

    val newHubsCount = withDroppedHubs.size - institutionsData.size
    val contributorsCount = withDroppedHubs.values.flatMap(_.institutions).size
    val newContributorsCount =
      contributorsCount - institutionsData.values.flatMap(_.institutions).size

    println(f"${newHubs.size} hubs found. $newHubsCount new hubs added.")
    println(
      f"$contributorsCount contributors found. $newContributorsCount new contributors added."
    )

    val outJson = map2jvalueHub(withDroppedHubs)

    Files.write(
      Paths.get("src/main/resources/wiki/institutions_v2.json"),
      pretty(outJson).getBytes(StandardCharsets.UTF_8)
    )
  }

  def updatedHub(hubName: String, prevHub: Hub): Hub = {
    val contributorNames = getContributorNames(hubName)
    val newContributors = contributorNames
      .map(name =>
        name -> prevHub.institutions.getOrElse(name, ContributingInstitution())
      )
      .toMap

    val oldContributors = prevHub.institutions.keys.flatMap(contributorName =>
      if (!contributorNames.contains(contributorName))
        Some(contributorName -> prevHub.institutions(contributorName))
      else None
    )
    val institutions = newContributors ++ oldContributors

    prevHub.copy(institutions = institutions)
  }
}
