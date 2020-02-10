package dpla.ingestion3.enrichments

import dpla.ingestion3.model.URI

object TaggingUtils {

  implicit class Tags(value: String) {

    /**
      * Applies at standard tag for records to be included in the PanAm portal
      */
    lazy val applyAviationTags: Option[URI] = {
      // tag value to apply
      val aviationTag = URI("aviation")

      // Values for which the aviation tag will be applied
      val taggingValues = Seq(
        "This item was digitized as part of the \"Cleared to Land\" project, supported by a grant from the National Historical Publications & Records Commission (NHPRC).",

        "This item was digitized as part of the \"Digitizing the 'World's Most Experienced Airline'\" project, supported by a grant from the Council on Library and Information Resources (CLIR). The grant program is made possible by funding from The Andrew W. Mellon Foundation."
      )

      if (taggingValues.contains(value)) {
        Some(aviationTag)
      } else {
        None
      }
    }

  }
}
