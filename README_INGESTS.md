Running DPLA Cultural Heritage ingests
----
 
- Helpful links and tools
- Scheduling 
- Running ingests on Vagrant EC2 instance 
- Running ingests locally 
- Exceptions / Usual ingests 

## Helpful links and tools
* [Hub ingest schedule](https://digitalpubliclibraryofamerica.atlassian.net/wiki/spaces/CT/pages/84969744/Hub+Re-ingest+Schedule)
* [Ingestion3 configuration](https://github.com/dpla/ingestion3-conf/)
* [xmll](https://github.com/dpla/xmll)
* [`screen` command](https://lazyprogrammer.me/tutorial-how-to-use-linux-screen/)
* [`vim` quickstart ](https://eastmanreference.com/a-quick-start-guide-for-beginners-to-the-vim-text-editor)
* [OhMyZsh `copyfile` plugin](https://github.com/ohmyzsh/ohmyzsh/tree/master/plugins/copyfile) 
* [jq](https://stedolan.github.io/jq/)
* [pyoaiharvester](https://github.com/dpla/pyoaiharvester)
* [vagrant](https://www.vagrantup.com/)
* [aws cli](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
* s3cmd

## Scheduling email
Monthly scheduling emails are sent to the hubs a month before they are scheduled to be run. We have used a common template for those scheduling emails to remind folks about the Wikimedia project and available documentation.

```text
Good morning - 

Here is the ingest schedule for ____.

1. Going forward we will trying to run all ingests on the last week 
of the month. 

2. We have automated our search index rebuild process 
and have it scheduled to run on Thursday evenings. You can expect 
your records to be available online the Friday of the week your 
ingest is scheduled to run.

3. Please let us know number of records to expect in your harvest and 
if you are providing a file export to DPLA, please let us know when 
we can expect delivery of the data to your s3 bucket

4. If you are participating in our Wikimedia project we will try to 
run an upload of images into Wikimedia the week following your DPLA 
metadata ingest. If you have any questions about that project, please 
contact our Data Fellow, Dominic, at dominic@dp.la

5. Lastly - we have updated some of the documentation for our ingestion 
system to provide a high level overview of how we map, normalize, 
validate and enrich your metadata, available at 
https://github.com/dpla/ingestion3. This documentation also unpacks some of those 
summary/error messages you are all quite familiar with. Please let me know 
if you have any questions about those stages or documentation.

The Wikimedia uploads will happen sometime in ____.  Please let us know 
if anything needs to be updated and, if possible, the number of records 
we can expect in our harvest.


April 24-28th
- Hub A
- Hub B
```

## Running ingests on EC2 Vagrant box 
We use [Vagrant](https://www.vagrantup.com/) to build the EC2 instance and install all required dependencies for running metadata ingests.  

1. Bring up the Vagrant ingest box  
```shell
> cd ~/ingestion3
> vagrant up
```
2. Create the i3.conf file inside ingestion3/conf/ folder 
```shell
# run on local copy of i3.conf file
> copyfile ./ingestion3/confi/i3.conf
# run on Vagrant box
> vagrant ssh
> touch ~/ingestion3/conf/i3.conf
> vim ~/ingestion3/conf/i3.conf
# paste i3.conf file contents into this file
# save the file
```
3. Start screen sessions for each provider you need to harvest. This makes re-attaching to running screen sessions much easier.
```shell
> cd ~/ingestion3/
> screen -S gpo  
> i3-harvest gpo
# Detach from session Ctrl+A, d

# Re-attach to gpo session
> screen -xS gpo
```
4. Sync data back to s3
```shell
> cd ~/data/gpo
> aws s3 sync . s3://dpla-master-dataset/gpo/
```

5. Run `vagrant destroy` when the ingests are finished, the data has been synced to s3 and the instance is no longer needed. 

## Running ingests locally
Bringing up the Vagrant EC2 instance is not always required. You can run a lot of the ingests on your laptop to save $$$ and overhead. Typically, these would be low record count harvests. Providers like Maryland, Vermont, or Digital Virginias which have ~150,000 records. 

## Exceptions and unusual ingests
Not all ingests are fire and forget, some require a bit of massaging before we can successfully harvest their data.
### Firewalled endpoints
Some hubs have their feed endpoints behind a firewall so the harvests needs to be run while behind out VPN. I've been meaning to try and get the EC2 instance behind the VPN but that work is not a high priority right now because we have a workaround (run locally behind VPN). Hubs which need to be harvested will on the VPN
- Illinois
- Indiana

### Community Webs
Internet Archive community webs will send us a SQL database which we need to open and export as a JSON file. Then convert the JSON file to JSONL

- [Install SQLite DB Brower](https://sqlitebrowser.org/dl/)
- Install `jq` if needed, `brew install jq` 

**Steps**
1. Open `.sql` file they emailed in SQLite DB Browser and export to JSON `File -> Export -> JSON`
2. Convert JSON to JSONL `jq -c '.[]' community-web-json-export.json > community-webs-jsonl-export.jsonl`
3. Zip the JSONL file `zip cw.zip community-webs-jsonl-export.jsonl`
4. Update the i3.conf file with the new endpoint/location of the zip file, e.g.  `./community-webs/originalRecords/20230426/`
### Digital Virginias
Digital Virginias uses [multiple Github repositories](https://github.com/dplava) to publish their metadata. We need to clone these repos in order to harvest their metadata. This is a handy invocation to clone all the repositories for a Github account. 

```shell
> cd ./data/virginias/originalRecords/20230425/
> curl -s https://[GITHUB_USER]:@api.github.com/orgs/dplava/repos\?per_page\=200 | jq ".[].ssh_url" | xargs -n 1 git clone
> zip -r -X ./dva-20230426.zip .
```

Then execute the harvest after updating the `virginas.harvest.endpoint` value in `i3.conf`

### NARA
Please see the [NARA specific documentation](README_NARA.md)

### Smithsonian
Please see the [Smithsonian specific documentation](README_SMITHSONIAN.md)
 
### Common errors
A list of common errors seen during ingests.
#### Zip file bug 
If you see this error in a file harvest stacktrace there is a bug when loading zip files created on Windows.

```shell
java.lang.IllegalArgumentException: Size must be equal or greater than zero: -1
    at org.apache.commons.io.IOUtils.toByteArray(IOUtils.java:505)
```
The easiest solution is simply to unzip and zip the file OSX using command line tools and then rerun the ingest
```shell
> unzip target.zip
> zip -r -X ./target-20230426.zip .
```