package dpla.ingestion3.enrichments

import dpla.ingestion3.model.URI

object TaggingUtils {

  implicit class Tags(value: String) {

    lazy val applyWisconsinGovernmentTags: Option[URI] = {
      val wisconsinTag = URI("wisconsin_gov_doc")
      val taggingValues = Seq(
        "p267601coll4",
        "p16831coll2",
        "p16831coll3",
        "p16831coll4",
        "p16831coll5",
        "p16831coll6",
        "p16831coll8"
      )

      if (taggingValues.contains(value)) {
        Some(wisconsinTag)
      } else {
        None
      }

    }

    /** Applies at standard tag for records to be included in the PanAm portal
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

    /** Applies tag for NWDH local
      */
    lazy val applyNwdhTags: Option[URI] = {
      val nwdhTag = URI("nwdh")

      // NWDH data providers in MWDL
      val providers = Seq(
        "Bushnell University",
        "Center for Asian Pacific Studies, University of Oregon",
        "African Studies Program, University of Oregon",
        "Department of Classics and the Department of the History of Art and Architecture, University of Oregon",
        "Emerald Media Group",
        "Jordan Schnitzer Museum of Art and the National Endowmant for the Humanities",
        "Latino Roots Project and the Center for Latino/a American Studies, University of Oregon, and SELCO Community Credit Union",
        "National Endowment for the Humanities and the American Council of Learned Societies",
        "Oregon State Highway Department",
        "Oregon State University Libraries",
        "Randall V. Mills Archives of Northwest Folklore and the Folklore Program, University of Oregon",
        "University of Central Florida, and Shandong University of Art and Design",
        "University of Oregon Libraries"
      )

      if (providers.contains(value)) {
        Some(nwdhTag)
      } else {
        None
      }
    }
  }
}
