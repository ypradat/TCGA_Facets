#!/bin/bash

sudo mkdir /home/ypradat
cd /home/ypradat

batch_index=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/batch_index -H "Metadata-Flavor: Google")
zone=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/zone -s | cut -d/ -f4)
instance_id=$(gcloud compute instances describe $(hostname) --zone=${zone} --format="get(id)")
gcloud_log_name=startup-gcloud-vm-${batch_index}
local_log_name=startup_gcloud_vm_${batch_index}.log
preempted=/home/ypradat/preempted.done

if [[-f "$preempted"]]; then
    exec 3>&1 4>&2 >>/home/ypradat/${local_log_name} 2>&1

    now_date="$(date +'%d/%m/%Y')"
    now_time="$(date +'%T')"
    printf "\nStart date and time after preemption: %s %s\n" "$now_date" "$now_time"

    # run the pipeline, rerunning incomplete jobs
    snakemake -s workflow/Snakefile --profile ./profile --resources load=115 --jobs 50 -f --rerun-incomplete

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
    gcloud compute instances delete $(hostname) --zone=${zone} --delete-disks=all --quiet

else
    exec 3>&1 4>&2 >/home/ypradat/${local_log_name} 2>&1

    now_date="$(date +'%d/%m/%Y')"
    now_time="$(date +'%T')"
    printf "Start date and time: %s %s\n" "$now_date" "$now_time"
    printf "Instance id: %s\n" "$instance_id"
    printf "Instance name: %s\n\n" "$(hostname)"

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

    # set the path in order to find the conda command
    export PATH="/home/ypradat/miniconda3/bin:/home/ypradat/miniconda3/condabin:$PATH"

    # log message
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "set PATH manually done.", "PATH": "'$PATH'", "PWD": "'$PWD'"}' \
	--payload-type=json \
	--severity=INFO

    # install mamba
    source activate /home/ypradat/miniconda3
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
    gsutil -m cp -r gs://facets_tcga/external .
    gsutil -m cp -r gs://facets_tcga/resources .

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
    source activate ${snakemake_env_dir}

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
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "started pipeline."}' \
	--payload-type=json \
	--severity=INFO

    # run the pipeline
    snakemake -s workflow/Snakefile --profile ./profile --resources load=115 --jobs 50 -f

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
    gcloud compute instances delete $(hostname) --zone=${zone} --delete-disks=all --quiet
fi
