# DPLA Ingestion 3

##Overview
This project is an ETL system for cultural heritage metadata. 

### Harvesters
* [OAI-PMH](#oai-pmh)
* ResourceSync
* API
* File

### Mappers
* ID minting  

### Enrichments
* String enrichments
* Linked data enrichments



## OAI-PMH
The OAI harvester takes its parameters from a config file that is passed in as a command line argument. 

### Example configuration file
```
type = "oai"
# this could be a local or remote path (S3://)
outputDir = "/some/path" 
endpoint = "http://example.oai.org/"
metadataPrefix = "mods"
provider = "dpla hub"
# One of these options must be set: blacklist, whitelist or harvestAllSets
blacklist = "badSet1,badSet2"
# whitelist = "" 
# harvestAllSets = true
verb = "ListRecords"
```

### Running using SBT
To use SBT you need to specify the path to the config file in sbtopts (the default location is /usr/local/etc/sbtopts)

```
# Exmple when the profile is stored remotely
-Dconfig.url=https://s3.amazonaws.com/dpla-i3-oai-profiles/sample_provider.conf

# Example when the path is stored locally
-Dconfig.file=/loca/path/to/config.conf

# Example when the config file is on the project classpath
-Dconfig.resource=profiles/sample_provider.conf
``` 

### Running in IntelliJ
Specify the config file parameter as a VM Option argument.

```text
-Dconfig.url=https://s3.amazonaws.com/dpla-i3-oai-profiles/sample_provider.conf
``` 

## ResourceSync 

## API

## File

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/6a9dfda51ad04ce3acfb7fcb441af846)](https://www.codacy.com/app/mdellabitta/ingestion3?utm_source=github.com&utm_medium=referral&utm_content=dpla/ingestion3&utm_campaign=badger)
[![Build Status](https://travis-ci.org/dpla/ingestion3.svg?branch=master)](https://travis-ci.org/dpla/ingestion3)
