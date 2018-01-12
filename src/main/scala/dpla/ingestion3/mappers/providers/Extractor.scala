//package dpla.ingestion3.mappers.providers
//
//import java.net.URI
//
//import dpla.ingestion3.model.{EdmAgent, OreAggregation}
//import org.apache.commons.codec.digest.DigestUtils
//
//import scala.util.{Failure, Success, Try}
//
///**
//  * Interface that all provider extractors implement.
//  */
//
//trait Extractor {
//
//  def build(): Try[OreAggregation]
//
//}
//
//case class ExtractorException(message: String) extends Exception(message)