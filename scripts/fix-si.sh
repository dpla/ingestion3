# Fix the Smithsonian data 
# 
data_path=${DPLA_DATA}/smithsonian/originalRecords/
ls "$data_path"
echo ''

# Get date from user input
read -p "Which folder: " date
echo "Reading from...$data_path$date"

mkdir -p "$data_path"/"$date"/fixed
mkdir -p "$data_path"/"$date"/xmll

# unzip and rezip
find $data_path$date/ -name "*.gz" -type f | \
  # shellcheck disable=SC2016
  xargs -I{} sh -c 'echo "$1" "./$(basename ${1%.*}).${1##*.}"' -- {} | \
  xargs -n 2 -P 8 sh -c 'gunzip -dckv $0 | gzip -kv > '$data_path$date'/fixed/$1'

# xmll
find $data_path$date/fixed/ -name "*.gz" -type f | \
  xargs -I{} sh -c 'echo "$1" "'$data_path$date'/xmll/$(basename ${1%.*}).${1##*.}"' -- {} | \
  xargs -n 2 -P 8 sh -c 'java -jar ~/dpla/code/xmll/target/scala-2.13/xmll-assembly-0.1.jar doc $0 $1'

rm -r $data_path/$date/fixed
rm $data_path/$date/*.xml.gz
mv $data_path$date/xmll/*.xml.gz $data_path$date/
rm -r $data_path/$date/xmll

echo 'done'
