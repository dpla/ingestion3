package dpla.ingestion3.mappers.providers

import dpla.ingestion3.model.{DplaMapData, EdmAgent}

/**
  * Interface that all provider extractors implement.
  */

trait Extractor {
  def build(rawData: String): DplaMapData
  def agent: EdmAgent
}
