#!/bin/bash

sudo mkdir -p /home/ypradat
cd /home/ypradat

batch_index=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/batch_index -H "Metadata-Flavor: Google")
zone=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/zone -s | cut -d/ -f4)
instance_id=$(gcloud compute instances describe $(hostname) --zone=${zone} --format="get(id)")
gcloud_log_name=startup-gcloud-vm-${batch_index}
local_log_name=startup_gcloud_vm_${batch_index}.log

exec 3>&1 4>&2 >/home/ypradat/${local_log_name} 2>&1

now_date="$(date +'%d/%m/%Y')"
now_time="$(date +'%T')"
printf "Start date and time: %s %s\n" "$now_date" "$now_time"
printf "Instance id: %s\n" "$instance_id"

# log message
gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "Connected to the VM.", "PATH": "'$PATH'", "PWD": "'$PWD'"}' \
    --payload-type=json \
    --severity=INFO

# print full details about the instance
gcloud compute instances describe $(hostname) --zone=${zone}

# install git
sudo apt --assume-yes install git

# install wget
sudo apt --assume-yes install wget

# log message
gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "sudo apt install git/wget done.", "PATH": "'$PATH'", "PWD": "'$PWD'"}' \
    --payload-type=json \
    --severity=INFO

# install locales to avoid warning "/usr/bin/bash: warning: setlocale: LC_ALL: cannot change locale (en_US.UTF-8)"
sudo apt --assume-yes install locales
sudo dpkg-reconfigure -f noninteractive tzdata
sudo sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen
sudo sed -i -e 's/# nb_NO.UTF-8 UTF-8/nb_NO.UTF-8 UTF-8/' /etc/locale.gen
sudo sed -i -e 's/LANG=.*/LANG="nb_NO.UTF-8"/' /etc/default/locale 
sudo dpkg-reconfigure -f noninteractive locales

# log message
gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "sudo dpkg-reconfigure done.", "PATH": "'$PATH'", "PWD": "'$PWD'"}' \
    --payload-type=json \
    --severity=INFO

# install conda and mamba
mkdir -p miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda3/miniconda.sh
bash miniconda3/miniconda.sh -b -u -p miniconda3
rm -rf /home/ypradat/miniconda3/miniconda.sh

# log message
gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "install miniconda3 done.", "PATH": "'$PATH'", "PWD": "'$PWD'"}' \
    --payload-type=json \
    --severity=INFO

cat <<'EOF' >>/home/ypradat/.bashrc
# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/home/ypradat/miniconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/home/ypradat/miniconda3/etc/profile.d/conda.sh" ]; then
        . "/home/ypradat/miniconda3/etc/profile.d/conda.sh"
    else
        export PATH="/home/ypradat/miniconda3/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<
EOF

cat <<'EOF'>>/home/ypradat/.condarc
auto_activate_base: false
EOF

source /home/ypradat/.bashrc

# log message
gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "source .bashrc done.", "PATH": "'$PATH'", "PWD": "'$PWD'"}' \
    --payload-type=json \
    --severity=INFO

# install mamba
conda activate /home/ypradat/miniconda3
conda install -y -c conda-forge mamba

# log message
if [[ $(which mamba) ]]; then
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "install mamba successful.", "PATH": "'$PATH'", "PWD": "'$PWD'"}' \
	--payload-type=json \
	--severity=INFO

else
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "install mamba failed.", "PATH": "'$PATH'", "PWD": "'$PWD'"}' \
	--payload-type=json \
	--severity=WARNING
fi

# get the code
git clone https://ypradat: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@github.com/ypradat/FacetsTCGA.git /home/ypradat/FacetsTCGA
cd /home/ypradat/FacetsTCGA

# log message
gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "git clone FacetsTCGA done."}' \
    --payload-type=json \
    --severity=INFO

# get resources and external
# gsutil -m cp -r gs://facets_tcga/external .
# gsutil -m cp -r gs://facets_tcga/resources .

# log message
gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "gsutil cp external & resources done."}' \
    --payload-type=json \
    --severity=INFO

# prepare results folder
mkdir -p results/mapping

# prepare for running snakemake
snakemake_env_dir=/home/ypradat/miniconda3/envs/snakemake
mamba env create --prefix ${snakemake_env_dir} -f workflow/envs/snakemake.yaml

# log message
gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "create snakemake environment done."}' \
    --payload-type=json \
    --severity=INFO

# log message
if [[ -d "${snakemake_env_dir}" ]]; then
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "create snakemake env successful.", "PATH": "'$PATH'", "PWD": "'$PWD'"}' \
	--payload-type=json \
	--severity=INFO

else
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "create snakemake env failed.", "PATH": "'$PATH'", "PWD": "'$PWD'"}' \
	--payload-type=json \
	--severity=WARNING
fi

# activate snakemake and run
conda activate /home/ypradat/miniconda3/envs/snakemake

# select samples
awk -F '\t' -v i="${batch_index}" 'NR==1; {if($(NF)==i) print $0}' config/samples.all.tsv > config/samples.tsv
awk -F '\t' -v i="${batch_index}" 'NR==1; {if($(NF)==i) print $0}' config/tumor_normal_pairs.all.tsv > config/tumor_normal_pairs.tsv

# log message
gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "samples selection done."}' \
    --payload-type=json \
    --severity=INFO

# set permissions to user
sudo chown -R ypradat /home/ypradat

# add batch_index to config
echo "batch_index: ${batch_index}" >>config/config.yaml

# log message
gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "starting pipeline."}' \
    --payload-type=json \
    --severity=INFO

# run the pipeline
snakemake -s workflow/Snakefile --profile ./profile --resources load=100 -fn

# log message
gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "terminated pipeline."}' \
    --payload-type=json \
    --severity=INFO

# last messages
now_date="$(date +'%d/%m/%Y')"
now_time="$(date +'%T')"
printf "\nEnd date and time: %s %s\n" "$now_date" "$now_time"

# delete instance
# gcloud compute instances delete $(hostname) --zone=${zone} --delete-disks=all --quiet
