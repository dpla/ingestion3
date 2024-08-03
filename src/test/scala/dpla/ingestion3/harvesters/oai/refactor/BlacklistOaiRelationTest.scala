package dpla.ingestion3.harvesters.oai.refactor

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class BlacklistOaiRelationTest extends AnyFlatSpec with SharedSparkContext {

  private val oaiConfiguration = OaiConfiguration(
    Map(
      "verb" -> "ListRecords",
      "blacklist" -> "ennie,meenie,miney,moe"
    )
  )

  private val oaiMethods: OaiMethods = new OaiMethods with Serializable {

    override def parsePageIntoRecords(
        page: OaiPage,
        removeDeleted: Boolean
    ): Seq[OaiRecord] =
      Seq(
        OaiRecord("a", "document", Seq())
      )

    override def listAllRecordPages(): IterableOnce[OaiPage] =
      Seq() // not implemented

    override def listAllSetPages(): IterableOnce[OaiPage] = Seq(
      OaiPage("1"),
      OaiPage("2")
    )

    override def listAllRecordPagesForSet(set: OaiSet): Seq[OaiPage] =
      listAllSetPages()

    override def parsePageIntoSets(page: OaiPage): Seq[OaiSet] = Seq(
      OaiSet("1", ""),
      OaiSet("moe", "")
    )
  }

  private lazy val sqlContext = SparkSession.builder().getOrCreate().sqlContext
  private lazy val relation =
    new BlacklistOaiRelation(oaiConfiguration, oaiMethods)(sqlContext)

  "A BlacklistOaiRelation" should "build a scan using OaiMethods" in {
    val rdd = relation.buildScan()
    assert(rdd.count == 4)
  }

}
