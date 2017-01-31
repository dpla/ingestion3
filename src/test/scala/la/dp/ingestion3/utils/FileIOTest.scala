package la.dp.ingestion3.utils

import java.io.File

import org.scalatest.{FlatSpec, Matchers}


/**
  * Tests for the ingestion3 FileIO utility
  */
class FileIOTest extends FlatSpec with Matchers {
  "writeFile " should " create a file" in {
    val f = new File("./file.txt")
    FileIO.writeFile("Test", new java.io.File("./file.txt"))
    assert(f.exists)
    f.delete() // cleanup
  }

  "writeFiles " should " create two files" in {
    val files = Map[java.io.File,String]( new File("./file_1.txt") -> "Test one.",
                                          new File("./file_2.txt") -> "Test two")
    FileIO.writeFiles(files)
    files.foreach( f => {
      assert(f._1.exists)
      f._1.delete() // cleanup
    } )
  }
}
