package dpla.ingestion3

import java.net.URI

import dpla.ingestion3.model.DplaMapData.LiteralOrUri

package object model {

  val DcmiTypes =
    Set("collection", "dataset", "event", "image", "interactiveresource", "service", "software", "sound", "text")

  def nameOnlyAgent(string: String): EdmAgent = EdmAgent(name = Some(string))

  def nameOnlyPlace(string: String): EdmPlace = EdmPlace(name = Some(string))

  def stringOnlyTimeSpan(string: String): EdmTimeSpan = EdmTimeSpan(originalSourceDate = Seq(string))

  def nameOnlyConcept(string: String): SkosConcept = SkosConcept(concept = Some(string))

  def nameOnlyCollection(string: String): DcmiTypeCollection = DcmiTypeCollection(title = Some(string))

  def uriOnlyWebResource(uri: URI): EdmWebResource = EdmWebResource(uri = uri)

  def isDcmiType(string: String): Boolean = DcmiTypes.contains(string.toLowerCase.replaceAll(" ", ""))

  def eitherStringOrUri(string: String): LiteralOrUri = new Left(string)

  def eitherStringOrUri(uri: URI): LiteralOrUri = new Right(uri)

}
