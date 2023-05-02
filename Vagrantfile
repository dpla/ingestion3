# Installs/updates SDKs/packages as root
$script = <<-'SCRIPT'

yum -y update
yum -y upgrade
yum -y install git
yum -y install awscli

SCRIPT

# Installs SDKs/packages as ec2-user
$sdk_script = <<-'SCRIPT'

curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 8.0.342-amzn
sdk install scala 2.11.8
sdk install spark 2.4.7
sdk install sbt

cd $HOME
git clone https://github.com/dpla/ingestion3.git
mkdir $HOME/ingestion3/conf/
mkdir $HOME/data/

cd $HOME/ingestion3
sbt compile

echo 'i3-harvest ()
{
  SBT_OPTS=-Xmx15g sbt "runMain dpla.ingestion3.entries.ingest.HarvestEntry
    --output /home/ec2-user/data/
    --conf /home/ec2-user/ingestion3/conf/i3.conf
    --name $1
    --sparkMaster local[*]"
}

i3-remap ()
{
  SBT_OPTS=-Xmx12g sbt "runMain dpla.ingestion3.entries.ingest.IngestRemap
    --output /home/ec2-user/data/
    --conf /home/ec2-user/ingestion3/conf/i3.conf
    --name $1
    --input /home/ec2-user/data/$1/harvest/
    --sparkMaster local[*]"
}' | sudo tee -a "/home/ec2-user/.bashrc" > /dev/null

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
  config.vm.box = "dummy"
  config.vm.provider :aws do |aws, override|
    aws.access_key_id = ENV['AWS_ACCESS_KEY_ID']
    aws.secret_access_key = ENV['AWS_SECRET_ACCESS_KEY']
    aws.ami = 'ami-0201c8df31f1b7ead' # x86_64 to arm64
    aws.iam_instance_profile_name = 'ingestion3-spark'
    aws.security_groups = ['sg-58f1803d', 'sg-07cfcb840697354e3'] # default, staff-jenkins-flintrock-access
    aws.subnet_id = "subnet-e8c8f3c0" # main vpc
    aws.ssh_host_attribute = :private_ip_address
    aws.keypair_name = "general"
    aws.instance_type = "m6g.2xlarge"
    override.ssh.username = "ec2-user"
    override.vm.synced_folder ".", "/vagrant", disabled: true
    override.ssh.private_key_path = "~/.ssh/general.pem"
    aws.tags = {
        'Name' => 'ingest'
    }
    aws.block_device_mapping = [{
          'DeviceName' => "/dev/xvda",
          'Ebs.VolumeSize' => 500, # Size in GB
          'Ebs.DeleteOnTermination' => true,
          'Ebs.VolumeType' => "gp3"
        }]
  end
  config.vm.provision "shell", inline: $script
  config.vm.provision "shell", privileged: false, inline: $sdk_script
end
