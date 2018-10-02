package dpla.ingestion3.model

import java.net.URI

import dpla.ingestion3.messages.IngestMessage
import dpla.ingestion3.model.DplaMapData._
import org.json4s.{JNothing, JValue}

import scala.util.Try

/**
  * Contains type definitions that express cardinality of fields
  * and a union type for fields that can be literals or uris.
  */
object DplaMapData {
  type ZeroToMany[T] = Seq[T]
  type AtLeastOne[T] = Seq[T]
  type ZeroToOne[T] = Option[T]
  type ExactlyOne[T] = T
  type LiteralOrUri = Either[String, URI]
}

//Core Classes

/**
  * The aggregation of attributes that apply to the described resource as a whole,
  * grouped from edm:WebResource and dpla:SourceResource.
  *
  */
case class OreAggregation(
                           dplaUri: ExactlyOne[URI],
                           sourceResource: ExactlyOne[DplaSourceResource],
                           dataProvider: ExactlyOne[EdmAgent],
                           originalRecord: ExactlyOne[String],
                           hasView: ZeroToMany[EdmWebResource] = Seq(),
                           intermediateProvider: ZeroToOne[EdmAgent] = None,
                           isShownAt: ExactlyOne[EdmWebResource],
                           `object`: ZeroToOne[EdmWebResource] = None, // full size image
                           preview: ZeroToOne[EdmWebResource] = None, // thumbnail
                           provider: ExactlyOne[EdmAgent],
                           edmRights: ZeroToOne[URI] = None,
                           sidecar: JValue = JNothing,
                           messages: ZeroToMany[IngestMessage] = Seq[IngestMessage]()
                         )

/**
  * dpla:SourceResource is a subclass of "edm:ProvidedCHO," which comprises the described resources
  * (in EDM called "cultural heritage objects") about which DPLA collects descriptions.
  * It is here that attributes of the described resources are located,
  * not the digital representations of them.
  */
case class DplaSourceResource(
                               alternateTitle: ZeroToMany[String] = Seq(),
                               collection: ZeroToMany[DcmiTypeCollection] = Seq(),
                               contributor: ZeroToMany[EdmAgent] = Seq(),
                               creator: ZeroToMany[EdmAgent] = Seq(),
                               date: ZeroToMany[EdmTimeSpan] = Seq(),
                               description: ZeroToMany[String] = Seq(),
                               extent: ZeroToMany[String] = Seq(),
                               format: ZeroToMany[String] = Seq(),
                               genre: ZeroToMany[SkosConcept] = Seq(),
                               identifier: ZeroToMany[String] = Seq(),
                               language: ZeroToMany[SkosConcept] = Seq(),
                               place: ZeroToMany[DplaPlace] = Seq(),
                               publisher: ZeroToMany[EdmAgent] = Seq(),
                               relation: ZeroToMany[LiteralOrUri] = Seq(),
                               replacedBy: ZeroToMany[String] = Seq(),
                               replaces: ZeroToMany[String] = Seq(),
                               rights: AtLeastOne[String] = Seq(),
                               rightsHolder: ZeroToMany[EdmAgent] = Seq(),
                               subject: ZeroToMany[SkosConcept] = Seq(),
                               temporal: ZeroToMany[EdmTimeSpan] = Seq(),
                               title: AtLeastOne[String] = Seq(),
                               `type`: ZeroToMany[String] = Seq() //todo should be URIs?
                             )


// Context Classes

case class EdmWebResource(
                           uri: ExactlyOne[URI],
                           fileFormat: ZeroToMany[String] = Seq(),
                           dcRights: ZeroToMany[String] = Seq(),
                           edmRights: ZeroToOne[String] = None //todo should be a URI?
                         )


case class EdmAgent(
                     uri: ZeroToOne[URI] = None,
                     name: ZeroToOne[String] = None,
                     providedLabel: ZeroToOne[String] = None,
                     note: ZeroToOne[String] = None,
                     scheme: ZeroToOne[URI] = None,
                     exactMatch: ZeroToMany[URI] = Seq(),
                     closeMatch: ZeroToMany[URI] = Seq()
                   ) {
  def print: String = name.getOrElse("")
}

case class DcmiTypeCollection(
                               title: ZeroToOne[String] = None,
                               description: ZeroToOne[String] = None
                             )

case class SkosConcept(
                        concept: ZeroToOne[String] = None,
                        providedLabel: ZeroToOne[String] = None,
                        note: ZeroToOne[String] = None,
                        scheme: ZeroToOne[URI] = None,
                        exactMatch: ZeroToMany[URI] = Seq(),
                        closeMatch: ZeroToMany[URI] = Seq()
                      )

case class DplaPlace(
                      name: ZeroToOne[String] = None,
                      city: ZeroToOne[String] = None,
                      county: ZeroToOne[String] = None,
                      state: ZeroToOne[String] = None,
                      country: ZeroToOne[String] = None,
                      region: ZeroToOne[String] = None,
                      coordinates: ZeroToOne[String] = None
                    )

case class EdmTimeSpan(
                        originalSourceDate: ZeroToOne[String] = None,
                        prefLabel: ZeroToOne[String] = None,
                        begin: ZeroToOne[String] = None,
                        end: ZeroToOne[String] = None
                      )


case class URI(value: String) {
  def validate: Boolean = Try { new java.net.URI(value) }.isSuccess
  def print: String = value
}