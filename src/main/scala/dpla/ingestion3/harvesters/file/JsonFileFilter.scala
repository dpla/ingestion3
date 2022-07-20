package dpla.ingestion3.harvesters.file

import java.io.{File, FileFilter}

class JsonFileFilter extends FileFilter {
  override def accept(pathname: File): Boolean = pathname.getName.endsWith("json")
}