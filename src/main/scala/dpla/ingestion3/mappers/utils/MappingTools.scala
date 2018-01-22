package dpla.ingestion3.mappers.utils

import dpla.ingestion3.mappers.providers.PaMapping
import dpla.ingestion3.mappers.{Mapper, XmlMapper}
import dpla.ingestion3.model.OreAggregation

import scala.util.Try
import scala.xml.NodeSeq



trait IngestionProfile {
  type Document
  type Parser[Document]
  type Mapper[Document, Mapping]
  type Mapping[Document]

}

class PaProfile extends IngestionProfile {
  type Document = NodeSeq
  type Parser = XmlParser
  type Mapper = XmlMapper[Document, Mapping]    // FIXME says it cannot accept parameters
  type Mapping = PaMapping
}



// TODO Move this into sep file

trait IngestionOps[Profile <: IngestionProfile] {
  def getParser: Parser[Profile#Document]
  def getMapper: Mapper[Profile#Document, Mapping[Profile#Document]]
  def getExtractor: Mapping[Profile#Document]
  // TODO def getEnricher: Enricher
}


//This brings a ops implementation into scope based on the type tags in the context?
object IngestionOps {
  def apply[Profile <: IngestionProfile](implicit ops: IngestionOps[Profile]): IngestionOps[Profile] = ops
}

//This models an ingestion pipeline and is generic over the types involved
class MappingWorkflow {

  def performMapping[Profile <: IngestionProfile](data: String)(implicit ops: IngestionOps[Profile]):
  Try[OreAggregation] = {
    import ops._
    val parser = getParser
    val extractor = getExtractor
    val mapper = getMapper

    val document: Profile#Document = parser.parse(data)
    mapper.map(document, extractor)
  }
}

//the main() method for running a mapping
object MappingExecutor extends App {
  implicit val profile: PaProfile = new PaProfile() // FIXME -- see psudo code for diff
  val oreAggregation: Try[OreAggregation] = new MappingWorkflow().performMapping("")
}


