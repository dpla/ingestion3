package dpla.ingestion3.harvesters.oai.refactor

import java.io.File
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.charset.Charset
import scala.jdk.CollectionConverters._

class AllRecordsOaiRelationTest extends AnyFlatSpec with SharedSparkContext {

  private val oaiConfiguration = OaiConfiguration(Map("verb" -> "ListRecords"))

  private val oaiMethods: OaiMethods = new OaiMethods with Serializable {

    override def parsePageIntoRecords(
        page: OaiPage,
        removeDeleted: Boolean
    ): Seq[OaiRecord] = Seq(OaiRecord("a", "document", Seq()))

    override def listAllRecordPages(): Seq[OaiPage] = Seq(
      OaiPage("blah"),
      OaiPage("blah2"),
      OaiPage("blah3")
    )

    override def listAllRecordPagesForSet(
        set: OaiSet
    ): IterableOnce[OaiPage] = Seq()

    override def parsePageIntoSets(
        page: OaiPage
    ): IterableOnce[OaiSet] = Seq()

    def listAllSetPages(): IterableOnce[OaiPage] = Seq()
  }

  private lazy val sqlContext = SparkSession.builder().getOrCreate().sqlContext
  private lazy val relation =
    new AllRecordsOaiRelation(oaiConfiguration, oaiMethods)(sqlContext)

  "a AllRecordsOaiRelation" should "parse a CSV row into an OaiPage" in {
    val pageRow = Row("page", "abcd", "")
    val page = relation.handleCsvRow(pageRow)
    assert(
      page.page === pageRow.getString(1)
    )
  }

  it should "write OAI harvest results to a temp file" in {
    val tempFile = File.createTempFile("oai", "test")
    relation.cacheTempFile(tempFile)
    val lines = FileUtils
      .readLines(tempFile, Charset.forName("UTF-8"))
      .asScala
      .toIndexedSeq
    assert(lines(0) === "\"page\",\"blah\",\"\"")
    assert(lines(1) === "\"page\",\"blah2\",\"\"")
    assert(lines(2) === "\"page\",\"blah3\",\"\"")
    tempFile.delete
  }

  it should "parse a temp file and return an RDD of the contents as Row(Either[OaiError,OaiPage])" in {
    val tempFile = File.createTempFile("oai", "test")
    FileUtils.writeLines(
      tempFile,
      Seq("page,blah,", "page,blah2,", "page,blah3,").asJava
    )
    val data = relation.tempFileToRdd(tempFile).collect().toIndexedSeq
    assert(data.size === 3)
    assert(data(0) === Row(null, Row("a", "document", Seq()), null))
    tempFile.delete()
  }

  it should "parse Rows into OaiPages and OaiErrors" in {
    assert(
      relation.handleCsvRow(Row("page", "foo", "")) === OaiPage("foo")
    )
    assert(
      relation.handleCsvRow(Row("page", "sorry\nsucker", "")) ===
        OaiPage("sorry\nsucker")
    )
  }

  it should "render OaiPage into appropriate Seqs" in {
    assert(
      relation.pageToArray(OaiPage("pagey")) === Seq("page", "pagey", "")
    )
  }

}
