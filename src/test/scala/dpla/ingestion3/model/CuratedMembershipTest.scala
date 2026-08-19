package dpla.ingestion3.model

import org.scalatest.flatspec.AnyFlatSpec

class CuratedMembershipTest extends AnyFlatSpec {

  "fromResource" should "load the bundled membership snapshot" in {
    val membership = CuratedMembership.fromResource
    assert(membership.exhibitions.nonEmpty)
    assert(membership.primarySourceSets.nonEmpty)
  }

  it should "key every entry on a 32-char hex DPLA item ID" in {
    val membership = CuratedMembership.fromResource
    val ids =
      membership.exhibitions.keySet ++ membership.primarySourceSets.keySet
    assert(ids.forall(_.matches("^[0-9a-f]{32}$")))
  }

  it should "map every entry to non-blank slugs" in {
    // Slug naming belongs upstream; assert only what the script guarantees
    val membership = CuratedMembership.fromResource
    val slugs = (membership.exhibitions.values ++
      membership.primarySourceSets.values).flatten
    assert(slugs.nonEmpty)
    assert(slugs.forall(_.matches("""^\S+$""")))
  }

  it should "record when the snapshot was generated" in {
    val generated = CuratedMembership.fromResource.generated
    assert(generated.matches("""^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"""))
  }
}
