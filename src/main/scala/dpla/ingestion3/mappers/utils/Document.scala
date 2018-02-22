package dpla.ingestion3.mappers.utils

case class Document[T](value: T) {
  def get: T = value
}
