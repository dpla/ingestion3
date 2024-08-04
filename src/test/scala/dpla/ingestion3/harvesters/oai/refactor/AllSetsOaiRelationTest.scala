package dpla.ingestion3.harvesters.oai.refactor

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class AllSetsOaiRelationTest extends AnyFlatSpec with SharedSparkContext {

  private val oaiConfiguration = OaiConfiguration(Map("verb" -> "ListRecords"))

  private val oaiMethods: OaiMethods = new OaiMethods with Serializable {

    override def parsePageIntoRecords(
        page: OaiPage,
        removeDeleted: Boolean
    ): Seq[OaiRecord] = Seq(
      OaiRecord("a", "document", Seq())
    )

    override def listAllRecordPages(): Seq[Nothing] = Seq()

    override def listAllSetPages(): Seq[OaiPage] = Seq(
      OaiPage("1"),
      OaiPage("2")
    )

    override def listAllRecordPagesForSet(
        set: OaiSet
    ): Seq[OaiPage] = listAllSetPages()

    override def parsePageIntoSets(
        page: OaiPage
    ): Seq[OaiSet] = Seq(
      OaiSet("1", ""),
      OaiSet("2", "")
    )
  }

  private lazy val sqlContext = SparkSession.builder().getOrCreate().sqlContext
  private lazy val relation =
    new AllSetsOaiRelation(oaiConfiguration, oaiMethods)(sqlContext)

  "An AllSetsOaiRelation" should "build a scan using OaiMethods" in {
    val rdd = relation.buildScan()
    assert(rdd.count === 8)
  }
}
