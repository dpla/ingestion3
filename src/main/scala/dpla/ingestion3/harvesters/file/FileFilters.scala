package dpla.ingestion3.harvesters.file

import java.io.{File, FileFilter}

object FileFilters {

  class AvroFileFilter extends FileFilter {
    override def accept(pathname: File): Boolean =
      pathname.getName.endsWith("avro")
  }

  class CsvFileFilter extends FileFilter {
    override def accept(pathname: File): Boolean =
      pathname.getName.endsWith("csv")
  }

  class GzFileFilter extends FileFilter {
    override def accept(pathname: File): Boolean =
      pathname.getName.endsWith("gz")
  }

  class JsonFileFilter extends FileFilter {
    override def accept(pathname: File): Boolean =
      pathname.getName.endsWith("json")
  }

  class XmlFileFilter extends FileFilter {
    override def accept(pathname: File): Boolean =
      pathname.getName.endsWith("xml")
  }

  class ZipFileFilter extends FileFilter {
    override def accept(pathname: File): Boolean =
      pathname.getName.endsWith("zip")
  }
}
