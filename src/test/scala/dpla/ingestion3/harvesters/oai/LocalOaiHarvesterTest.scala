package dpla.ingestion3.harvesters.oai

import org.scalatest.flatspec.AnyFlatSpec

class LocalOaiHarvesterTest extends AnyFlatSpec {

  private val requestInfo = OaiRequestInfo("ListSets", None, None, None, 0L)

  private def set(spec: String, name: Option[String]): OaiSet =
    OaiSet(spec, "<set/>", requestInfo, name)

  "LocalOaiHarvester.setNamesFromSets" should "map each setSpec to its setName" in {
    val names = LocalOaiHarvester.setNamesFromSets(
      Iterator(
        set("publication:brewer_images", Some("Brewer Images")),
        set(
          "publication:regions",
          Some("Digital Repositories for Maine Communities")
        )
      )
    )
    assert(
      names === Map(
        "publication:brewer_images" -> "Brewer Images",
        "publication:regions" -> "Digital Repositories for Maine Communities"
      )
    )
  }

  it should "skip sets without a name" in {
    val names = LocalOaiHarvester.setNamesFromSets(
      Iterator(set("publication:unnamed", None))
    )
    assert(names.isEmpty)
  }

  "LocalOaiHarvester.setNameElements" should "emit a setName for each named header setSpec, in header order" in {
    val header =
      <header>
        <identifier>oai:digitalmaine.com:brewer_images-1010</identifier>
        <setSpec>publication:brewer_images</setSpec>
        <setSpec>publication:unknown</setSpec>
        <setSpec>publication:regions</setSpec>
      </header>
    val elements = LocalOaiHarvester.setNameElements(
      header,
      Map(
        "publication:regions" -> "Digital Repositories for Maine Communities",
        "publication:brewer_images" -> "Brewer Images"
      )
    )
    assert(
      elements.map(e => (e \@ "spec", e.text)) === Seq(
        "publication:brewer_images" -> "Brewer Images",
        "publication:regions" -> "Digital Repositories for Maine Communities"
      )
    )
  }

  "LocalOaiHarvester.SeenIds" should "accept an identifier once and count repeats" in {
    val seen = new LocalOaiHarvester.SeenIds
    assert(seen.firstTime("oai:digitalmaine.com:books-1012"))
    assert(seen.firstTime("oai:digitalmaine.com:books-1013"))
    assert(!seen.firstTime("oai:digitalmaine.com:books-1012"))
    assert(seen.duplicatesSkipped === 1)
  }
}
