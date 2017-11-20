package dpla.ingestion3.harvesters.oai.refactor

import java.io.File

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.FlatSpec

import scala.collection.JavaConversions._

class AllRecordsOaiRelationTest extends FlatSpec with SharedSparkContext {

  private val oaiConfiguration = OaiConfiguration(Map("verb" -> "ListRecords"))

  private val oaiMethods = new OaiMethods with Serializable {

    override def parsePageIntoRecords(pageEither: Either[OaiError, OaiPage]) = Seq(
      Right(OaiRecord("a", "document", Seq()))
    )

    override def listAllRecordPages() = Seq(
      Right(OaiPage("blah")),
      Right(OaiPage("blah2")),
      Right(OaiPage("blah3")),
      Left(OaiError("oops", None))
    )

    override def listAllSetPages() = ???

    override def listAllRecordPagesForSet(setEither: Either[OaiError, OaiSet]) = ???

    override def parsePageIntoSets(pageEither: Either[OaiError, OaiPage]) = ???
  }

  private lazy val relation = new AllRecordsOaiRelation(oaiConfiguration, oaiMethods)(sc)

  "a AllRecordsOaiRelation" should "parse a CSV row into an Either[OaiError, OaiPage]" in {
    val pageRow = Row("page", "abcd", null)
    val errorRow1 = Row("error", "abcd", null)
    val errorRow2 = Row("error", "efgh", "foo")

    val page = relation.handleCsvRow(pageRow)
    assert(page.isRight)
    assert(page.right.get.page === pageRow.getString(1))

    val error1 = relation.handleCsvRow(errorRow1)
    assert(error1.isLeft)
    assert(error1.left.get.message === errorRow1.getString(1))
    assert(error1.left.get.url === None)

    val error2 = relation.handleCsvRow(errorRow2)
    assert(error2.isLeft)
    assert(error2.left.get.message === errorRow2.getString(1))
    assert(error2.left.get.url === Some(errorRow2.getString(2)))
  }

  it should "write OAI harvest results to a temp file" in {
    val tempFile = File.createTempFile("oai", "test")
    relation.cacheTempFile(tempFile)
    val lines = FileUtils.readLines(tempFile).toIndexedSeq
    assert(lines(0) === "page,blah,")
    assert(lines(1) === "page,blah2,")
    assert(lines(2) === "page,blah3,")
    assert(lines(3) === "error,oops,")
    tempFile.delete
  }

  it should "parse a temp file and return an RDD of the contents as Row(Either[OaiError,OaiPage])" in {
    val tempFile = File.createTempFile("oai", "test")
    FileUtils.writeLines(tempFile, Seq("page,blah,", "page,blah2,", "page,blah3,", "error,oops,None"))
    val data = relation.tempFileToRdd(tempFile).collect().toIndexedSeq
    assert(data.size === 4)
    assert(data(0) === Row(None, OaiRecord("a", "document", Seq()), None))
    tempFile.delete()
  }

  it should "parse Rows into OaiPages and OaiErrors" in {
    assert(relation.handleCsvRow(Row("page", "foo", null)) === Right(OaiPage("foo")))
    assert(relation.handleCsvRow(Row("error", "sorry", null)) === Left(OaiError("sorry", None)))
  }

  it should "render Either[OaiError,OaiPage] into approprate Seqs" in {
    assert(
      relation.eitherToArray(Left(OaiError("sorry", None))) === Seq("error", "sorry", null)
    )
    assert(
      relation.eitherToArray(Left(OaiError("sorry", Some("bar")))) === Seq("error", "sorry", "bar")
    )
    assert(
      relation.eitherToArray(Right(OaiPage("pagey"))) === Seq("page", "pagey", null)
    )
  }

}
