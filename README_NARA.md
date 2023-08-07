# How to run NARA ingests

Latest version of the NARA harvester 

Fixes a major legacy issue of using the `DigitalObjectIdentifier` instead of the `naId` as the primary record identifier.  

**Preprocessing** 
1. Examine file export from NARA 
2. Move `deletes_.xml` files into ./deletes directory 
3. Rename "deletes_XXX.xml" to include a datestamp 
`> for f in *.xml ; do mv ./$f ./20200704_$f ; done`
4. Files are typically shipped as `.zip` this needs to be re-compressed as `.tar.gz` or `.bz2` after removing deletes files

Here is how the files should be organized before running a delta ingest
```
~/nara/delta/20200704/
-- /deletes/deletes_XXX.xml
-- 20200704_nara_delta.tar.gz
```

**Delta Harvest**
After preprocessing the data export from NARA run the `NaraDeltaHarvester`
1. Configure the `i3.conf` file to point to the delta directory 
```
nara.provider = "National Archives and Records Administration"
nara.harvest.type = "nara.file.delta"
nara.harvest.delta.update =   "~/nara/delta/20200704/"
```
Run NARA harvest

This will produce a harvest data set with all records provided in the delta (no deletions are performed at this step). 
`202000704_121258-nara-OriginalRecord.avro`. Move this into the delta directory. We want the deletes, original records and original harvest all alongside each other.

```
~/nara/delta/20200704/
-- /deletes/deletes_XXX.xml
-- 20200704_nara_delta.tar.gz
-- 202000704_121258-nara-OriginalRecord.avro
```

**Merge Utility**
The `NaraMergeUtil` will take two harvested data sets (base and delta) and merge the delta into the base data set. Using the last successful harvest as the base, and the output from the last step as the delta.
```
sbt "runMain dpla.ingestion3.utils.NaraMergeUtil 
~/nara/harvest/20200301_000000.avro # base
~/nara/delta/20200704/202000704_121258-nara-OriginalRecord.avro # delta
~/nara/delta/20200704/deletes/ 
~/nara/harvest/20200704_000000-nara-OriginalRecord.avro # merged output 
local[*]"
```

The merge utility will log details about the merge and delete operations (duplicates, updates, inserts, deletions, failed deletions) in a summary files (e.g. `~/nara/harvest/20200704_000000.avro/_LOGS/_SUMMARY.txt`)
```
 Base
 ----------
 path: ~/nara/harvest/20200301_000000.avro
 record count 14,468,257
 duplicate count 0
 unique count 14,468,257

 Delta
 -----------
 path: ~/nara/delta/20200704/202000704_121258-nara-OriginalRecord.avro 
 record count: 149,750
 duplicate count: 1
 unique count: 149,749

 Merged
 -----------
 path: ~/nara/harvest/20200704_000000.avro
 new: 54,412
 update: 95,337
 total [actual]: 14,522,669
 total [expected]: 14,522,669 = 14,468,257 (base) + 54,412 (new)

 Delete
 ------------
 path: ~/nara/delta/20200704/deletes/
 ids to delete specified at path: 109,015
 invalid deletes (IDs not in merged dataset): 103,509
 valid deletes (IDs in merged dataset): 5,506
 actual removed (merged total - total after deletes): 5,506

 Final
 -----
 total [actual]: 14,517,163 = 14,522,669 (merge actual) - 5,506 (delete actual)
```

Additionally, all of insert, update, delete operations are logged with the corresponding NARA IDs in a series of CSV files alongside the summary text file. 
```
id,operation
100104443,insert
100108988,update
100111103,delete
```

The final merged data set is ready for mapping/enrichment/topic_modeling/indexing.
