$script = <<-'SCRIPT'
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
sudo curl -o /etc/apt/trusted.gpg.d/sbt.asc -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823"
sudo apt-get -y update
sudo apt-get -y upgrade
sudo apt-get -y install git sbt openjdk-8-jdk
sudo apt-get -y install awscli

sudo curl -O -sL "https://mirrors.sonic.net/apache/spark/spark-2.4.8/spark-2.4.8-bin-hadoop2.7.tgz"
tar -xf spark-2.4.8-bin-hadoop2.7.tgz
sudo mv spark-2.4.8-bin-hadoop2.7.tgz /usr/local/spark
echo 'export PATH="$PATH:/usr/local/spark/bin"; export SPARK_HOME="/usr/local/spark"' | tee -a "/etc/bash.bashrc" > /dev/null

sudo echo 'i3-harvest ()
{
  SBT_OPTS=-Xmx15g sbt "runMain dpla.ingestion3.entries.ingest.HarvestEntry
    --output /home/ubuntu/data/
    --conf /home/ubuntu/ingestion3/conf/i3.conf
    --name $1
    --sparkMaster local[*]"
}

i3-remap ()
{
  SBT_OPTS=-Xmx12g sbt "runMain dpla.ingestion3.entries.ingest.IngestRemap
    --output /home/ubuntu/data/
    --conf /home/ubuntu/ingestion3/conf/i3.conf
    --name $1
    --input /home/ubuntu/data/$1/harvest/
    --sparkMaster local[*]"
}' | tee -a "/home/ubuntu/.bashrc" > /dev/null

cd ~/
sudo git clone https://github.com/dpla/ingestion3.git

SCRIPT

class Hash
  def slice(*keep_keys)
    h = {}
    keep_keys.each { |key| h[key] = fetch(key) if has_key?(key) }
    h
  end unless Hash.method_defined?(:slice)
  def except(*less_keys)
    slice(*keys - less_keys)
  end unless Hash.method_defined?(:except)
end


Vagrant.configure("2") do |config|
  #config.vm.box = "ubuntu/focal64"
  config.vm.box = "dummy"
  config.vm.provider :aws do |aws, override|
    aws.access_key_id = ENV['AWS_ACCESS_KEY_ID']
    aws.secret_access_key = ENV['AWS_SECRET_ACCESS_KEY']
    aws.ami = 'ami-0885b1f6bd170450c'
    aws.iam_instance_profile_name = 'ingestion3-spark'
    aws.security_groups = ['sg-58f1803d', 'sg-07cfcb840697354e3'] # default, staff-jenkins-flintrock-access
    aws.subnet_id = "subnet-e8c8f3c0"
    aws.ssh_host_attribute = :private_ip_address
    aws.keypair_name = "general"
    aws.instance_type = "m6g.2xlarge"
    override.ssh.username = "ubuntu"
    override.vm.synced_folder ".", "/vagrant", disabled: true 
    override.ssh.private_key_path = "~/.ssh/general.pem"
    aws.tags = {
        'Name' => 'ingest'
    }
  end
  config.vm.provision "shell", inline: $script
end
