package dpla.ingestion3.enrichments

import dpla.ingestion3.model.URI

object TagUtils {

  implicit class Tags(value: String) {

    /**
      *
      */
    lazy val applyPanAmTags: Option[URI] = {
      // Values for which the PanAm tag will be applied
      val taggingValues = Seq(
        "This item was digitized as part of the \"Cleared to Land\" project, supported by a grant from the National Historical Publications & Records Commission (NHPRC).",

        "This item was digitized as part of the \"Digitizing the 'World's Most Experienced Airline'\" project, supported by a grant from the Council on Library and Information Resources (CLIR). The grant program is made possible by funding from The Andrew W. Mellon Foundation."
      )

      val panAmTag = URI("PanAm")

      if (taggingValues.contains(value)) {
        Some(panAmTag)
      } else {
        None
      }
    }

  }
}
