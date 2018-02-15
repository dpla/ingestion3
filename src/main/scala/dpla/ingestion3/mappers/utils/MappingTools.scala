package dpla.ingestion3.mappers.utils

import dpla.ingestion3.profiles.PaProfile

case class Document[T](value: T) {
  def get: T = value
}




// the main() method for running a mapping
object MappingExecutor extends App {
  val profile: PaProfile = new PaProfile()

  val xml =
    <record>
      <metadata>
        <header>
          <identifier>1234</identifier>
        </header>
        <rights>http://google.com</rights>
      </metadata>
    </record>



  // val rights = profile.getMapping.edmRights(Document(xml))

  val mappedRecord = profile.performMapping(xml.toString())

  println(mappedRecord)
}


