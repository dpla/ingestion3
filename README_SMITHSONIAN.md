# How to run Smithsonian ingests

**Preprocessing**
1. Download latest export from SI in s3://dpla-hub-si/ 
2. Recompress files, there is some kind of issue the gzip files produced by their export scripts.

   `> mkdir ./fixed/`
   ```> find ./si/originalRecords/ -name "*.gz" -type f | xargs -I{} sh -c 'echo "$1" "./$(basename ${1%.*}).${1##*.}"' -- {} | xargs -n 2 -P 8 sh -c 'gunzip -dckv $0 | gzip -kv > ./si/originalRecords/fixed/$1'```
3. Run `xmll` over the fixed files [dpla/xmll github project](https://github.com/dpla/xmll)

   `> mkdir ./si/originalRecords/xmll`
   
   ```> find ./si/originalRecords/fixed/ -name "*.gz" -type f | xargs -I{} sh -c 'echo "$1" "./xmll/$(basename ${1%.*}).${1##*.}"' -- {} | xargs -n 2 -P 8 sh -c 'java -jar ~/dpla/code/xmll/target/scala-2.13/xmll-assembly-0.1.jar doc $0 $1'```

4. The original downloads and fixed files can be deleted and only the xmll'd need to be retained for harvesting
5. Run standard Smithsonian ingestion3 harvest 