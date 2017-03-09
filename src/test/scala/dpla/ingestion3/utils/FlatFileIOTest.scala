package dpla.ingestion3.utils

import org.scalatest.{FlatSpec, Matchers}

/**
  * Tests for the ingestion3 FileIO utility
  */
class FlatFileIOTest extends FlatSpec with Matchers {
  val fileIO = new FlatFileIO
  "writeFile " should " create a file" in {
    val fName = "ingestion3-FlatFileIoTest.txt"
    val f = new java.io.File(fName)

    fileIO.writeFile("Test", fName)
    assert(f.exists)
    f.delete()
  }
}
