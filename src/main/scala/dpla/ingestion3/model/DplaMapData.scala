package dpla.ingestion3.model

import java.net.URI

import dpla.ingestion3.model.DplaMapData._

/**
  * Contains type definitions that express cardinality of fields
  * and a union type for fields that can be literals or uris.
  */

object DplaMapData {
  type ZeroToMany[T] = Seq[T]
  type AtLeastOne[T] = Seq[T]
  type ZeroToOne[T] = Option[T]
  type ExactlyOne[T] = T
  type LiteralOrUri = Either[String,URI]
  type LiteralOrSkos = Either[String,SkosConcept]
}

/**
  * Container for the classes that represent an item in DPLA MAP.
  *
  * @see https://dp.la/info/developers/map/
  *
  * @param sourceResource Metadata about the source item rather than it's representations.
  * @param edmWebResource Metadata about the item's representation on the provider's site.
  * @param oreAggregation Metadata about the aggretation of the item across it's source and representations.
  */
case class DplaMapData(
                         sourceResource: DplaSourceResource,
                         edmWebResource: EdmWebResource,
                         oreAggregation: OreAggregation
                       )

//Core Classes

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
                               place: ZeroToMany[EdmPlace] = Seq(), //specified as dpla:Place in the spec
                               publisher: ZeroToMany[EdmAgent] = Seq(),
                               relation: ZeroToMany[LiteralOrUri] = Seq(),
                               replacedBy: ZeroToMany[String] = Seq(),
                               replaces: ZeroToMany[String] = Seq(),
                               rights: AtLeastOne[String] = Seq(),
                               rightsHolder: ZeroToMany[EdmAgent] = Seq(),
                               subject: ZeroToMany[SkosConcept] = Seq(),
                               temporal: ZeroToMany[EdmTimeSpan] = Seq(),
                               title: AtLeastOne[String] = Seq(),
                               `type`: ZeroToMany[String] = Seq() //should be URIs?
                             )

/**
  * Contains the attributes of the digital representation of the web resource, not the source resource.
  *
  * Used both to talk about the item in situ on the provider's site, along with other entities on the Web.
  */
case class EdmWebResource(
                           uri: ExactlyOne[URI],
                           fileFormat: ZeroToMany[String] = Seq(),
                           dcRights: ZeroToMany[String] = Seq(),
                           edmRights: ZeroToOne[String] = None //todo should be a URI?
                         )

/**
  * The aggregation of attributes that apply to the described resource as a whole,
  * grouped from edm:WebResource and dpla:SourceResource.
  *
  */

case class OreAggregation(
                           /*
                            * FIXME: it's not clear what `uri' corresponds to
                            * in Section 4.1.C of
                            * http://dp.la/info/wp-content/uploads/2015/03/MAPv4.pdf
                            */
                           uri: ExactlyOne[URI], //uri of the record on our site
                           dataProvider: ExactlyOne[EdmAgent],
                           originalRecord: ExactlyOne[String], //map v4 specifies this as a ref, but that's LDP maybe?
                           hasView: ZeroToMany[EdmWebResource] = Seq(),
                           intermediateProvider: ZeroToOne[EdmAgent] = None,
                           `object`: ZeroToOne[EdmWebResource] = None, //full item page
                           preview: ZeroToOne[EdmWebResource] = None, ///thumbnail
                           provider: ExactlyOne[EdmAgent],
                           edmRights: ZeroToOne[URI] = None
                         )


// Context Classes

case class EdmAgent(
                     uri: ZeroToOne[URI] = None,
                     name: ZeroToOne[String] = None,
                     providedLabel: ZeroToOne[String] = None,
                     note: ZeroToOne[String] = None,
                     scheme: ZeroToOne[URI] = None,
                     exactMatch: ZeroToMany[URI] = Seq(),
                     closeMatch: ZeroToMany[URI] = Seq()
                   )

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

case class EdmPlace(
                     name: ZeroToOne[String] = None,
                     provideLabel: ZeroToOne[String] = None,
                     latitude: ZeroToOne[String] = None, //these possibly should be a numeric type?
                     longitude: ZeroToOne[String] = None, // this too
                     altitude: ZeroToOne[String] = None, //this three
                     geometry: ZeroToOne[String] = None, //cowardly calling this a string. not currently used.
                     parentFeature: ZeroToMany[EdmPlace] = Seq(),
                     countryCode: ZeroToOne[String] = None, //maybe come up with a enum thingy for this
                     note: ZeroToOne[String] = None,
                     scheme: ZeroToOne[URI] = None,
                     exactMatch: ZeroToMany[SkosConcept] = Seq(),
                     closeMatch: ZeroToMany[SkosConcept] = Seq()
                   )

case class EdmTimeSpan(
                        /* Why is this 0.n? I would have assumed ExactlyOne or ZeroToOne
                            since it wouldn't make sense to have multiple original ranges
                            per EdmTimeSpan but rather one edmTimeSpan per originalSourceDate?

                            Esp. since in date in sourceResource is also ZeroToMany...

                            I'm changing the type for the enrichment driver and need to follow-up
                            w/team

                            Also added a prefLabel property to store the enriched label
                         */
                        originalSourceDate: ZeroToOne[String] = None,
                        prefLabel: ZeroToOne[String] = None,
                        begin: ZeroToOne[String] = None,
                        end: ZeroToOne[String] = None
                      )


