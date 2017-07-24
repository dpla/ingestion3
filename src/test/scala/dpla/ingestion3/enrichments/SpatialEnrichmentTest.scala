package dpla.ingestion3.enrichments

import org.json4s.jackson.JsonMethods
import dpla.ingestion3.model.DplaPlace
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, PrivateMethodTester}
import util.Try


class SpatialEnrichmentTest extends FlatSpec with PrivateMethodTester
    with MockFactory {

  val geocoder: Twofisher = mock[Twofisher]
  val e = new SpatialEnrichment(geocoder)

  val jsonCity: String =
    """
      |{
      |    "interpretations": [
      |        {
      |            "what": "",
      |            "where": "somerville ma",
      |            "feature": {
      |                "cc": "US",
      |                "geometry": {
      |                    "center": {
      |                        "lat": 42.3876,
      |                        "lng": -71.0995
      |                    },
      |                    "bounds": {
      |                        "ne": {
      |                            "lat": 42.418118,
      |                            "lng": -71.072728
      |                        },
      |                        "sw": {
      |                            "lat": 42.372724,
      |                            "lng": -71.134514
      |                        }
      |                    },
      |                    "source": "qs"
      |                },
      |                "name": "Somerville",
      |                "displayName": "Somerville, MA, United States",
      |                "woeType": 7,
      |                "ids": [
      |                    {
      |                        "source": "geonameid",
      |                        "id": "4951257"
      |                    },
      |                    {
      |                        "source": "woeid",
      |                        "id": "2495739"
      |                    }
      |                ],
      |                "names": [
      |                    {
      |                        "name": "Somerville",
      |                        "lang": "en",
      |                        "flags": [
      |                            16,
      |                            1
      |                        ]
      |                    }
      |                ],
      |                "highlightedName": "<b>Somerville</b>, <b>MA</b>, United States",
      |                "matchedName": "Somerville, MA, United States",
      |                "id": "geonameid:4951257",
      |                "attributes": {
      |                    "population": 75754,
      |                    "urls": [
      |                        "http://en.wikipedia.org/wiki/Somerville%2C_Massachusetts"
      |                    ]
      |                },
      |                "longId": "72057594042879193",
      |                "parentIds": [
      |                    "72057594044179937",
      |                    "72057594044182862",
      |                    "72057594042871845"
      |                ]
      |            },
      |            "parents": [
      |                {
      |                    "cc": "US",
      |                    "geometry": {
      |                        "center": {
      |                            "lat": 42.48555,
      |                            "lng": -71.39184
      |                        },
      |                        "bounds": {
      |                            "ne": {
      |                                "lat": 42.73667,
      |                                "lng": -71.020377
      |                            },
      |                            "sw": {
      |                                "lat": 42.156782,
      |                                "lng": -71.898716
      |                            }
      |                        },
      |                        "source": "tiger"
      |                    },
      |                    "name": "Middlesex County",
      |                    "displayName": "Middlesex County, MA, United States",
      |                    "woeType": 9,
      |                    "ids": [
      |                        {
      |                            "source": "geonameid",
      |                            "id": "4943909"
      |                        },
      |                        {
      |                            "source": "woeid",
      |                            "id": "12588708"
      |                        }
      |                    ],
      |                    "names": [
      |                        {
      |                            "name": "Middlesex",
      |                            "lang": "en",
      |                            "flags": [
      |                                16,
      |                                8,
      |                                1
      |                            ]
      |                        },
      |                        {
      |                            "name": "Middlesex County",
      |                            "lang": "en",
      |                            "flags": [
      |                                16,
      |                                1
      |                            ]
      |                        }
      |                    ],
      |                    "id": "geonameid:4943909",
      |                    "attributes": {
      |                        "population": 1503085,
      |                        "urls": [
      |                            "http://en.wikipedia.org/wiki/Middlesex_County%2C_Massachusetts",
      |                            "http://en.wikipedia.org/wiki/Middlesex_%2C_Massachusetts"
      |                        ]
      |                    },
      |                    "longId": "72057594042871845",
      |                    "parentIds": [
      |                        "72057594044179937",
      |                        "72057594044182862"
      |                    ]
      |                },
      |                {
      |                    "cc": "US",
      |                    "geometry": {
      |                        "center": {
      |                            "lat": 42.36565,
      |                            "lng": -71.10832
      |                        },
      |                        "bounds": {
      |                            "ne": {
      |                                "lat": 42.88679,
      |                                "lng": -69.85886099999999
      |                            },
      |                            "sw": {
      |                                "lat": 41.187053,
      |                                "lng": -73.50814199999999
      |                            }
      |                        },
      |                        "source": "usa_adm1.shp"
      |                    },
      |                    "name": "Massachusetts",
      |                    "displayName": "Massachusetts, United States",
      |                    "woeType": 8,
      |                    "ids": [
      |                        {
      |                            "source": "geonameid",
      |                            "id": "6254926"
      |                        },
      |                        {
      |                            "source": "woeid",
      |                            "id": "2347580"
      |                        }
      |                    ],
      |                    "names": [
      |                        {
      |                            "name": "Massachusetts",
      |                            "lang": "en",
      |                            "flags": [
      |                                128,
      |                                16,
      |                                1
      |                            ]
      |                        },
      |                        {
      |                            "name": "Commonwealth of Massachusetts",
      |                            "lang": "en",
      |                            "flags": [
      |                                16
      |                            ]
      |                        },
      |                        {
      |                            "name": "Bay State",
      |                            "lang": "en",
      |                            "flags": [
      |                                64,
      |                                16
      |                            ]
      |                        },
      |                        {
      |                            "name": "MA",
      |                            "lang": "abbr",
      |                            "flags": [
      |                                2
      |                            ]
      |                        }
      |                    ],
      |                    "id": "geonameid:6254926",
      |                    "attributes": {
      |                        "population": 6433422,
      |                        "urls": [
      |                            "http://id.loc.gov/authorities/names/n79007084",
      |                            "http://en.wikipedia.org/wiki/Massachusetts"
      |                        ]
      |                    },
      |                    "longId": "72057594044182862",
      |                    "parentIds": [
      |                        "72057594044179937"
      |                    ]
      |                },
      |                {
      |                    "cc": "US",
      |                    "geometry": {
      |                        "center": {
      |                            "lat": 39.76,
      |                            "lng": -98.5
      |                        },
      |                        "bounds": {
      |                            "ne": {
      |                                "lat": 72.781306,
      |                                "lng": 180
      |                            },
      |                            "sw": {
      |                                "lat": 15.265251,
      |                                "lng": -180
      |                            }
      |                        },
      |                        "source": "gn-adm0-new3.json"
      |                    },
      |                    "name": "United States",
      |                    "displayName": "United States",
      |                    "woeType": 12,
      |                    "ids": [
      |                        {
      |                            "source": "geonameid",
      |                            "id": "6252001"
      |                        }
      |                    ],
      |                    "names": [
      |                        {
      |                            "name": "US",
      |                            "lang": "abbr",
      |                            "flags": [
      |                                2
      |                            ]
      |                        },
      |                        {
      |                            "name": "United States of America",
      |                            "lang": "en",
      |                            "flags": [
      |                                16
      |                            ]
      |                        },
      |                        {
      |                            "name": "USA",
      |                            "lang": "abbr",
      |                            "flags": [
      |                                2
      |                            ]
      |                        },
      |                        {
      |                            "name": "U.S.",
      |                            "lang": "abbr",
      |                            "flags": [
      |                                2
      |                            ]
      |                        },
      |                        {
      |                            "name": "U.S.A.",
      |                            "lang": "abbr",
      |                            "flags": [
      |                                2
      |                            ]
      |                        },
      |                        {
      |                            "name": "United States",
      |                            "lang": "en",
      |                            "flags": [
      |                                64,
      |                                16,
      |                                1
      |                            ]
      |                        },
      |                        {
      |                            "name": "America",
      |                            "lang": "en",
      |                            "flags": [
      |                                16
      |                            ]
      |                        }
      |                    ],
      |                    "id": "geonameid:6252001",
      |                    "attributes": {
      |                        "population": 310232863,
      |                        "urls": [
      |                            "http://id.loc.gov/authorities/names/n78095330",
      |                            "http://it.wikipedia.org/wiki/Stati_Uniti_d%27America",
      |                            "http://www.whitehouse.gov/",
      |                            "http://www.usa.gov/",
      |                            "http://de.wikipedia.org/wiki/Vereinigte_Staaten",
      |                            "http://tr.wikipedia.org/wiki/Amerika_Birle%C5%9Fik_Devletleri",
      |                            "http://no.wikipedia.org/wiki/USA",
      |                            "http://ru.wikipedia.org/wiki/%D0%A1%D0%BE%D0%B5%D0%B4%D0%B8%D0%BD%D1%91%D0%BD%D0%BD%D1%8B%D0%B5_%D0%A8%D1%82%D0%B0%D1%82%D1%8B_%D0%90%D0%BC%D0%B5%D1%80%D0%B8%D0%BA%D0%B8",
      |                            "http://en.wikipedia.org/wiki/United_States"
      |                        ]
      |                    },
      |                    "longId": "72057594044179937",
      |                    "parentIds": [
      |                        "72057594044183085"
      |                    ]
      |                }
      |            ]
      |        }
      |    ]
      |}
    """.stripMargin
  val jvalueCity: org.json4s.JsonAST.JValue = JsonMethods.parse(jsonCity)

  val jsonCounty: String =
    """
      |{
      |    "interpretations": [
      |        {
      |            "what": "",
      |            "where": "tulare county ca",
      |            "feature": {
      |                "cc": "US",
      |                "geometry": {
      |                    "center": {
      |                        "lat": 36.22016,
      |                        "lng": -118.80047
      |                    },
      |                    "bounds": {
      |                        "ne": {
      |                            "lat": 36.744816,
      |                            "lng": -117.980761
      |                        },
      |                        "sw": {
      |                            "lat": 35.789161,
      |                            "lng": -119.573194
      |                        }
      |                    },
      |                    "source": "tiger"
      |                },
      |                "name": "Tulare County",
      |                "displayName": "Tulare County, CA, United States",
      |                "woeType": 9,
      |                "ids": [
      |                    {
      |                        "source": "geonameid",
      |                        "id": "5403789"
      |                    },
      |                    {
      |                        "source": "woeid",
      |                        "id": "12587723"
      |                    }
      |                ],
      |                "names": [
      |                    {
      |                        "name": "Tulare County",
      |                        "lang": "en",
      |                        "flags": [
      |                            16,
      |                            1
      |                        ]
      |                    },
      |                    {
      |                        "name": "Tulare",
      |                        "lang": "en",
      |                        "flags": [
      |                            16,
      |                            8,
      |                            1
      |                        ]
      |                    }
      |                ],
      |                "highlightedName": "<b>Tulare County</b>, <b>CA</b>, United States",
      |                "matchedName": "Tulare County, CA, United States",
      |                "id": "geonameid:5403789",
      |                "attributes": {
      |                    "population": 442179,
      |                    "urls": [
      |                        "http://en.wikipedia.org/wiki/Tulare_County%2C_California",
      |                        "http://en.wikipedia.org/wiki/Tulare_%2C_California"
      |                    ]
      |                },
      |                "longId": "72057594043331725",
      |                "parentIds": [
      |                    "72057594044179937",
      |                    "72057594043260857"
      |                ]
      |            },
      |            "parents": [
      |                {
      |                    "cc": "US",
      |                    "geometry": {
      |                        "center": {
      |                            "lat": 37.25022,
      |                            "lng": -119.75126
      |                        },
      |                        "bounds": {
      |                            "ne": {
      |                                "lat": 42.009516999999995,
      |                                "lng": -114.131211
      |                            },
      |                            "sw": {
      |                                "lat": 32.528832,
      |                                "lng": -124.48200299999999
      |                            }
      |                        },
      |                        "source": "usa_adm1.shp"
      |                    },
      |                    "name": "California",
      |                    "displayName": "California, United States",
      |                    "woeType": 8,
      |                    "ids": [
      |                        {
      |                            "source": "geonameid",
      |                            "id": "5332921"
      |                        },
      |                        {
      |                            "source": "woeid",
      |                            "id": "2347563"
      |                        }
      |                    ],
      |                    "names": [
      |                        {
      |                            "name": "State of California",
      |                            "lang": "en",
      |                            "flags": [
      |                                16
      |                            ]
      |                        },
      |                        {
      |                            "name": "Calif",
      |                            "lang": "abbr",
      |                            "flags": [
      |                                2
      |                            ]
      |                        },
      |                        {
      |                            "name": "California",
      |                            "lang": "en",
      |                            "flags": [
      |                                128,
      |                                16,
      |                                1
      |                            ]
      |                        },
      |                        {
      |                            "name": "California Republic",
      |                            "lang": "en",
      |                            "flags": [
      |                                16
      |                            ]
      |                        },
      |                        {
      |                            "name": "Golden State",
      |                            "lang": "en",
      |                            "flags": [
      |                                16
      |                            ]
      |                        },
      |                        {
      |                            "name": "CA",
      |                            "lang": "abbr",
      |                            "flags": [
      |                                2
      |                            ]
      |                        }
      |                    ],
      |                    "id": "geonameid:5332921",
      |                    "attributes": {
      |                        "population": 37691912,
      |                        "urls": [
      |                            "http://en.wikipedia.org/wiki/California"
      |                        ]
      |                    },
      |                    "longId": "72057594043260857",
      |                    "parentIds": [
      |                        "72057594044179937"
      |                    ]
      |                },
      |                {
      |                    "cc": "US",
      |                    "geometry": {
      |                        "center": {
      |                            "lat": 39.76,
      |                            "lng": -98.5
      |                        },
      |                        "bounds": {
      |                            "ne": {
      |                                "lat": 72.781306,
      |                                "lng": 180
      |                            },
      |                            "sw": {
      |                                "lat": 15.265251,
      |                                "lng": -180
      |                            }
      |                        },
      |                        "source": "gn-adm0-new3.json"
      |                    },
      |                    "name": "United States",
      |                    "displayName": "United States",
      |                    "woeType": 12,
      |                    "ids": [
      |                        {
      |                            "source": "geonameid",
      |                            "id": "6252001"
      |                        }
      |                    ],
      |                    "names": [
      |                        {
      |                            "name": "US",
      |                            "lang": "abbr",
      |                            "flags": [
      |                                2
      |                            ]
      |                        },
      |                        {
      |                            "name": "United States of America",
      |                            "lang": "en",
      |                            "flags": [
      |                                16
      |                            ]
      |                        },
      |                        {
      |                            "name": "USA",
      |                            "lang": "abbr",
      |                            "flags": [
      |                                2
      |                            ]
      |                        },
      |                        {
      |                            "name": "U.S.",
      |                            "lang": "abbr",
      |                            "flags": [
      |                                2
      |                            ]
      |                        },
      |                        {
      |                            "name": "U.S.A.",
      |                            "lang": "abbr",
      |                            "flags": [
      |                                2
      |                            ]
      |                        },
      |                        {
      |                            "name": "United States",
      |                            "lang": "en",
      |                            "flags": [
      |                                64,
      |                                16,
      |                                1
      |                            ]
      |                        },
      |                        {
      |                            "name": "America",
      |                            "lang": "en",
      |                            "flags": [
      |                                16
      |                            ]
      |                        }
      |                    ],
      |                    "id": "geonameid:6252001",
      |                    "attributes": {
      |                        "population": 310232863,
      |                        "urls": [
      |                            "http://id.loc.gov/authorities/names/n78095330",
      |                            "http://it.wikipedia.org/wiki/Stati_Uniti_d%27America",
      |                            "http://www.whitehouse.gov/",
      |                            "http://www.usa.gov/",
      |                            "http://de.wikipedia.org/wiki/Vereinigte_Staaten",
      |                            "http://tr.wikipedia.org/wiki/Amerika_Birle%C5%9Fik_Devletleri",
      |                            "http://no.wikipedia.org/wiki/USA",
      |                            "http://ru.wikipedia.org/wiki/%D0%A1%D0%BE%D0%B5%D0%B4%D0%B8%D0%BD%D1%91%D0%BD%D0%BD%D1%8B%D0%B5_%D0%A8%D1%82%D0%B0%D1%82%D1%8B_%D0%90%D0%BC%D0%B5%D1%80%D0%B8%D0%BA%D0%B8",
      |                            "http://en.wikipedia.org/wiki/United_States"
      |                        ]
      |                    },
      |                    "longId": "72057594044179937",
      |                    "parentIds": [
      |                        "72057594044183085"
      |                    ]
      |                }
      |            ]
      |        }
      |    ]
      |}
    """.stripMargin
  val jvalueCounty: org.json4s.JsonAST.JValue = JsonMethods.parse(jsonCounty)

  val jsonCountry: String =
    """
      |{
      |    "interpretations": [
      |        {
      |            "what": "",
      |            "where": "greece",
      |            "feature": {
      |                "cc": "GR",
      |                "geometry": {
      |                    "center": {
      |                        "lat": 39,
      |                        "lng": 22
      |                    },
      |                    "bounds": {
      |                        "ne": {
      |                            "lat": 41.75,
      |                            "lng": 31.5
      |                        },
      |                        "sw": {
      |                            "lat": 33.713797,
      |                            "lng": 18
      |                        }
      |                    },
      |                    "source": "gn-adm0-new3.json"
      |                },
      |                "name": "Greece",
      |                "displayName": "Greece",
      |                "woeType": 12,
      |                "ids": [
      |                    {
      |                        "source": "geonameid",
      |                        "id": "390903"
      |                    }
      |                ],
      |                "names": [
      |                    {
      |                        "name": "GR",
      |                        "lang": "abbr",
      |                        "flags": [
      |                            2
      |                        ]
      |                    },
      |                    {
      |                        "name": "Greece",
      |                        "lang": "en",
      |                        "flags": [
      |                            128,
      |                            64,
      |                            16,
      |                            1
      |                        ]
      |                    },
      |                    {
      |                        "name": "Hellenic Republic",
      |                        "lang": "en",
      |                        "flags": [
      |                            16
      |                        ]
      |                    }
      |                ],
      |                "highlightedName": "<b>Greece</b>",
      |                "matchedName": "Greece",
      |                "id": "geonameid:390903",
      |                "attributes": {
      |                    "population": 11000000,
      |                    "urls": [
      |                        "http://ru.wikipedia.org/wiki/%D0%93%D1%80%D0%B5%D1%86%D0%B8%D1%8F",
      |                        "http://en.wikipedia.org/wiki/Greece"
      |                    ]
      |                },
      |                "longId": "72057594038318839",
      |                "parentIds": [
      |                    "72057594044183084"
      |                ]
      |            },
      |            "parents": [
      |                {
      |                    "cc": "",
      |                    "geometry": {
      |                        "center": {
      |                            "lat": 48.69096,
      |                            "lng": 9.14062
      |                        },
      |                        "bounds": {
      |                            "ne": {
      |                                "lat": 80.83253479,
      |                                "lng": 40.1438713074
      |                            },
      |                            "sw": {
      |                                "lat": 27.4497890472,
      |                                "lng": -31.2595367432
      |                            }
      |                        }
      |                    },
      |                    "name": "Europe",
      |                    "displayName": "Europe",
      |                    "woeType": 29,
      |                    "ids": [
      |                        {
      |                            "source": "geonameid",
      |                            "id": "6255148"
      |                        }
      |                    ],
      |                    "names": [
      |                        {
      |                            "name": "Europe",
      |                            "lang": "en",
      |                            "flags": [
      |                                1
      |                            ]
      |                        }
      |                    ],
      |                    "id": "geonameid:6255148",
      |                    "attributes": {
      |                        "population": 0,
      |                        "urls": [
      |                            "http://id.loc.gov/authorities/subjects/sh85045631",
      |                            "http://en.wikipedia.org/wiki/Europe"
      |                        ]
      |                    },
      |                    "longId": "72057594044183084",
      |                    "parentIds": [
      |                        "72057594044223566"
      |                    ]
      |                }
      |            ]
      |        }
      |    ]
      |}
    """.stripMargin
  val jvalueCountry: org.json4s.JsonAST.JValue = JsonMethods.parse(jsonCountry)

  val jsonNoplace: String = """{"interpretations": []}"""
  val jvalueNoplace: org.json4s.JsonAST.JValue = JsonMethods.parse(jsonNoplace)


  "SpatialEnrichment.queryTerm" should "return coordinates if they are " +
      "present" in {
    val queryTerm = PrivateMethod[Option[String]]('queryTerm)
    val place = DplaPlace(name=Some("x"), coordinates=Some("1,2"))
    val rv = e invokePrivate queryTerm(place)
    assert(rv.contains("1,2"))
  }

  it should "return name if coordinates are not present" in {
    val queryTerm = PrivateMethod[Option[String]]('queryTerm)
    val place = DplaPlace(name=Some("x"))
    val rv = e invokePrivate queryTerm(place)
    assert(rv.contains("x"))
  }

  it should "return empty for 'United States' (leading to no lookup)" in {
    val queryTerm = PrivateMethod[Option[String]]('queryTerm)
    val place = DplaPlace(name=Some("United States"))
    val rv = e invokePrivate queryTerm(place)
    assert(rv.isEmpty)
  }

  it should "return the name for 'United States--<some state>'" in {
    val queryTerm = PrivateMethod[Option[String]]('queryTerm)
    val place = DplaPlace(name=Some("United States--Kansas"))
    val rv = e invokePrivate queryTerm(place)
    assert(rv.contains("United States--Kansas"))
  }

  "SpatialEnrichment.name" should "return the interpretation's display " +
      "name (city)" in {
    val displayName = PrivateMethod[Option[String]]('displayName)
    val rv = e invokePrivate displayName(jvalueCity)
    assert(rv.contains("Somerville, MA, United States"))
  }

  it should "return the interpretation's display name (county, etc.)" in {
    val displayName = PrivateMethod[Option[String]]('displayName)
    val rv = e invokePrivate displayName(jvalueCounty)
    assert(rv.contains("Tulare County, CA, United States"))
  }

  "SpatialEnrichment.parentFeatureName" should "return parent name for given " +
      "woeType" in {
    val parentFeatureName = PrivateMethod[Option[String]]('parentFeatureName)
    val rv = e invokePrivate parentFeatureName(jvalueCity, 12)
    assert(rv.contains("United States"))
  }

  it should "not fail, but be undefined if the woeType is irrelevant" in {
    val parentFeatureName = PrivateMethod[Option[String]]('parentFeatureName)
    val rv = e invokePrivate parentFeatureName(jvalueCity, 0)
    assert(rv.isEmpty)
  }

  "SpatialEnrichment.city" should "return the Place's city" in {
    val city = PrivateMethod[Option[String]]('city)
    val rv = e invokePrivate city(jvalueCity)
    assert(rv.contains("Somerville"))
  }

  "SpatialEnrichment.county" should "return the Place's county" in {
    val county = PrivateMethod[Option[String]]('county)
    val rv = e invokePrivate county(jvalueCity)
    assert(rv.contains("Middlesex County"))
  }

  it should "fill in the county if given a result for a county" in {
    /*
     * The subtlety here being that these feature-level methods don't just look
     * for a _parent_ feature for the county, but also consider whether the
     * feature itself is a county. The other methods for state and country work
     * the same.
     */
    val county = PrivateMethod[Option[String]]('county)
    val rv = e invokePrivate county(jvalueCounty)
    assert(rv.contains("Tulare County"))
  }

  "SpatialEnrichment.state" should "return the Place's state" in {
    val state = PrivateMethod[Option[String]]('state)
    val rv = e invokePrivate state(jvalueCity)
    assert(rv.contains("Massachusetts"))
  }

  "SpatialEnrichment.country" should "return the Place's country" in {
    val country = PrivateMethod[Option[String]]('country)
    val rv = e invokePrivate country(jvalueCity)
    assert(rv.contains("United States"))
  }

  "SpatialEnrichment.coordinates" should "return the Place's coordinates" in {
    val coordinates = PrivateMethod[Option[String]]('coordinates)
    val rv = e invokePrivate coordinates(jvalueCity)
    assert(rv.contains("42.3876,-71.0995"))
  }

  "SpatialEnrichment.realPlaceName" should "return the looked-up place " +
      "name instead of coordinates" in {
    val realPlaceName = PrivateMethod[Option[String]]('realPlaceName)
    val rv = e invokePrivate realPlaceName(
      Some("42.3876,-71.0995"),
      jvalueCity,
      true
    )
    assert(rv.contains("Somerville, MA, United States"))
  }

  it should "return the provider's preferred place name if it's not " +
      "coordinates" in {
    val realPlaceName = PrivateMethod[Option[String]]('realPlaceName)
    val rv = e invokePrivate realPlaceName(Some("x"), jvalueCity, false)
    assert(rv.contains("x"))
  }

  "SpatialEnrichment.enrich" should "return a copy of the given DplaPlace" in {
    geocoder.geocoderResponse _ expects(*, *) returns jvalueCity
    val origPlace = DplaPlace(name=Option("Somerville, MA"))
    val newPlace = e.enrich(origPlace)
    assert(newPlace != origPlace)
  }

  it should "not clobber an existing value" in {
    geocoder.geocoderResponse _ expects(*, *) returns jvalueCity
    val origPlace = DplaPlace(name=Option("x"))
    val newPlace = e.enrich(origPlace)
    // What we really care about ...
    assert(newPlace.name.contains("x"))
    // And just to prove the place really was updated ...
    assert(newPlace.state.contains("Massachusetts"))
  }

  it should "not return the result for a country" in {
    geocoder.geocoderResponse _ expects(*, *) returns jvalueCountry
    val origPlace = DplaPlace(name=Option("Greece"))
    val newPlace = e.enrich(origPlace)
    assert(newPlace == origPlace)
  }

  it should "call Twofishes with correct query params for place name" in {
    geocoder.geocoderResponse _ expects("query", "x") returns jvalueCity
    val place = DplaPlace(name=Some("x"))
    e.enrich(place)
  }

  it should "call Twofishes with correct query params for coordinates" in {
    geocoder.geocoderResponse _ expects("ll", "40.0,-70.0") returns jvalueCity
    val place = DplaPlace(name=Some("40,-70"))
    e.enrich(place)
  }

  /*
   * com.github.dvdme.GeoCoordinatesLib lacks unit tests. We're going to verify
   * that it works with the formats that we say we'll support in
   * https://docs.google.com/document/d/1b2iJI90I24hUp-8kCfnZhAQcefBt0vPUjMzhDn9HOZ0/edit?usp=sharing
   */
  "SpatialEnrichment.coordinatesFromString" should
      "handle coordinates given in DPLA-approved formats" in {
    // Note also variations in whitespace ...
    val coordStrings = Seq(
      "34° 37' 6.024\" N, 79° 0' 37.62\" W",
      "34 37' 6.024\" N, 79 0' 37.62\" W",
      "34 37m 6.024s N, 79 0m 37.62s W",
      " 34.61834N,79.01045W",
      "34.61834 ,-79.01045 "
    )
    for (s <- coordStrings) {
      val f = PrivateMethod[Option[String]]('coordinatesFromString)
      val rv = e invokePrivate f(s)
      assert(rv.contains("34.61834,-79.01045"))
    }
  }

  "SpatialEnrichment.woeType" should "not blow up on empty JSON" in {
    val woeType = PrivateMethod[Try[Int]]('woeType)
    val rv = e invokePrivate woeType(jvalueNoplace)
    assert(rv.getOrElse(0) == 0)
  }

  it should "return the right woeType for a successful lookup" in {
    val woeType = PrivateMethod[Try[Int]]('woeType)
    val rv = e invokePrivate woeType(jvalueCity)
    assert(rv.getOrElse(0) == 7)
  }
}
