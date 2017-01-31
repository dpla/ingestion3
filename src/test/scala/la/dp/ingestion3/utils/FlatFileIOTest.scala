package la.dp.ingestion3.utils

import java.io.File

import org.scalatest.{FlatSpec, Matchers}


/**
  * Tests for the ingestion3 FileIO utility
  */
class FlatFileIOTest extends FlatSpec with Matchers {
  val fileIO = new FlatFileIO
  "writeFile " should " create a file" in {
    val f = File.createTempFile("ingestion3-FlatFileIoTest", ".txt")

    fileIO.writeFile("Test", f)
    assert(f.exists)
  }

  "writeFiles " should " create two files" in {
    val files = Map[java.io.File,String]( File.createTempFile("file_1", ".txt") -> "Test one",
                                          File.createTempFile("file_2", ".txt") -> "Test two")
    fileIO.writeFiles(files)
    files.foreach{
      case(file: File, str: String) => {
        assert(file.exists)
      }
    }
  }
}
