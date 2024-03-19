package dpla.ingestion3.harvesters.oai.refactor

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class AllSetsOaiRelationTest extends AnyFlatSpec with SharedSparkContext {

  private val oaiConfiguration = OaiConfiguration(Map("verb" -> "ListRecords"))

  private val oaiMethods: OaiMethods = new OaiMethods with Serializable {

    override def parsePageIntoRecords(pageEither: Either[OaiError, OaiPage], removeDeleted: Boolean): Seq[Right[Nothing, OaiRecord]] = Seq(
      Right(OaiRecord("a", "document", Seq()))
    )

    override def listAllRecordPages(): Seq[Nothing] = Seq()

    override def listAllSetPages(): Seq[Right[Nothing, OaiPage]] = Seq(
      Right(OaiPage("1")),
      Right(OaiPage("2"))
    )

    override def listAllRecordPagesForSet(setEither: Either[OaiError, OaiSet]): Seq[Right[Nothing, OaiPage]] = listAllSetPages()

    override def parsePageIntoSets(pageEither: Either[OaiError, OaiPage]): Seq[Right[Nothing, OaiSet]] = Seq(
      Right(OaiSet("1", "")), Right(OaiSet("2", ""))
    )
  }

  private lazy val sqlContext = SparkSession.builder().getOrCreate().sqlContext
  private lazy val relation = new AllSetsOaiRelation(oaiConfiguration, oaiMethods)(sqlContext)


  "An AllSetsOaiRelation" should "build a scan using OaiMethods" in {
    val rdd = relation.buildScan()
    assert(rdd.count === 8)
  }
}
