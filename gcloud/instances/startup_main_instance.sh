#!/bin/bash

cd /home/ypradat

zone=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/zone -s | cut -d/ -f4)
instance_id=$(gcloud compute instances describe $(hostname) --zone=${zone} --format="get(id)")
gcloud_log_vm=/home/ypradat/startup_gcloud_vm.log

exec 3>&1 4>&2 >${gcloud_log_vm} 2>&1

now_date="$(date +'%d/%m/%Y')"
now_time="$(date +'%T')"
printf "Start date and time: %s %s\n" "$now_date" "$now_time"
printf "Instance id: %s\n" "$instance_id"
printf "Instance name: %s\n\n" "$(hostname)"

# print full details about the instance
gcloud compute instances describe $(hostname) --zone=${zone}

# install git
sudo apt --assume-yes install git

# install wget
sudo apt --assume-yes install wget

# install screen
sudo apt --assume-yes install screen

# install locales to avoid warning "/usr/bin/bash: warning: setlocale: LC_ALL: cannot change locale (en_US.UTF-8)"
sudo apt --assume-yes install locales
sudo dpkg-reconfigure -f noninteractive tzdata
sudo sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen
sudo sed -i -e 's/# nb_NO.UTF-8 UTF-8/nb_NO.UTF-8 UTF-8/' /etc/locale.gen
sudo sed -i -e 's/LANG=.*/LANG="nb_NO.UTF-8"/' /etc/default/locale 
sudo dpkg-reconfigure -f noninteractive locales

# install conda and mamba
mkdir -p miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda3/miniconda.sh
bash miniconda3/miniconda.sh -b -u -p miniconda3
rm -rf /home/ypradat/miniconda3/miniconda.sh

# set the path in order to find the conda command
export PATH="/home/ypradat/miniconda3/bin:/home/ypradat/miniconda3/condabin:$PATH"

# activate
source activate /home/ypradat/miniconda3

# get the code
git clone https://ypradat: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@github.com/ypradat/FacetsTCGA.git /home/ypradat/FacetsTCGA

# get resources and external
cd /home/ypradat/FacetsTCGA

# set permissions to user
sudo chown -R ypradat /home/ypradat
