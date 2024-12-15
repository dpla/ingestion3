package dpla.ingestion3.harvesters.file

import java.io.{File, FileFilter}

object FileFilters {

  private def newFilter(ext: String): FileFilter =
    (pathname: File) => pathname.getName.endsWith(ext)

  val avroFilter: FileFilter = newFilter("avro")
  val gzFilter: FileFilter = newFilter("gz")
  val xmlFilter: FileFilter = newFilter("xml")
  val zipFilter: FileFilter = newFilter("zip")
}
