package dpla.ingestion3.harvesters.oai.refactor

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, FunSuite}

class WhitelistOaiRelationTest extends FlatSpec with SharedSparkContext {

  private val oaiConfiguration = OaiConfiguration(Map(
    "verb" -> "ListRecords",
    "setlist" -> "ennie,meenie,miney,moe"
  ))

  private val oaiMethods = new OaiMethods with Serializable {

    override def parsePageIntoRecords(pageEither: Either[OaiError, OaiPage], removeDeleted: Boolean) = Seq(
      Right(OaiRecord("a", "document", Seq()))
    )

    override def listAllRecordPages = listAllSetPages()

    override def listAllSetPages() = Seq(
      Right(OaiPage("1")),
      Right(OaiPage("2"))
    )

    override def listAllRecordPagesForSet(setEither: Either[OaiError, OaiSet]) = listAllSetPages()

    override def parsePageIntoSets(pageEither: Either[OaiError, OaiPage]) = Seq(
      Right(OaiSet("1", "")), Right(OaiSet("moe", ""))
    )
  }

  private lazy val sqlContext = SparkSession.builder().getOrCreate().sqlContext
  private lazy val relation = new WhitelistOaiRelation(oaiConfiguration, oaiMethods)(sqlContext)


  "A WhitelistOaiRelation" should "build a scan using OaiMethods" in {
    val rdd = relation.buildScan()
    assert(rdd.count === 8)
  }


}
