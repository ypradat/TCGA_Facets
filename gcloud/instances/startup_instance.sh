#!/bin/bash

user=ypradat
batch_index=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/batch_index -H "Metadata-Flavor: Google")
zone=$(curl -H Metadata-Flavor:Google http://metadata.google.internal/computeMetadata/v1/instance/zone -s | cut -d/ -f4)
instance_id=$(gcloud compute instances describe $(hostname) --zone=${zone} --format="get(id)")
gcloud_log_name=startup-gcloud-vm-${batch_index}
gcloud_log_vm=/home/${user}/startup_gcloud_vm_${batch_index}.log
snakemake_env_dir=/home/${user}/miniconda3/envs/snakemake

function join_by {
  local d=${1-} f=${2-}
  if shift 2; then
    printf %s "$f" "${@/#/$d}"
  fi
}

home_content=$(ls /home/${user}/)
home_content=$(join_by , $home_content)

# log message
gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "home content '${home_content}'"}' \
    --payload-type=json \
    --severity=INFO

if [[ -f "${gcloud_log_vm}" ]]; then
    # If the startup log already exists, we hypothesize that the VM was stopped for some reason (e.g preemption). In
    # this case, we try to restart the snakemake pipeline from where it left off. Of note, if the VM was stopped before
    # all the tools and dependencies required for the pipeline were installed, the code will simply fail and the VM will
    # have to be recreated.

    exec 3>&1 4>&2 >>${gcloud_log_vm} 2>&1

    # log message
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "starting again a preempted job."}' \
	--payload-type=json \
	--severity=INFO

    now_date="$(date +'%d/%m/%Y')"
    now_time="$(date +'%T')"
    printf "\nStart date and time after preemption: %s %s\n" "$now_date" "$now_time"

    # set the path in order to find the conda command
    export PATH="/home/${user}/miniconda3/bin:/home/${user}/miniconda3/condabin:$PATH"

    # activate snakemake and run
    cd /home/${user}/FacetsTCGA
    source activate ${snakemake_env_dir}

    # run the pipeline, rerunning incomplete jobs
    snakemake -s workflow/Snakefile --profile ./profile --resources load=115 --jobs 50 -f --unlock
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

    # before deleting, check that the log was uploaded to the results folder.
    # if not, upload it to the failed folder
    gsutil -q stat gs://facets_tcga_results/logs/gcloud/startup_gcloud_vm_${batch_index}.log
    status=$?

    if [[ $status != 0 ]]; then
	gsutil cp ${gcloud_log_vm} gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_${batch_index}.log
    fi

    # delete instance
    gcloud compute instances delete $(hostname) --zone=${zone} --delete-disks=all --quiet

else
    # First startup of the VM, we install all tools and dependencies required for the pipeline before downloading the
    # pipeline and starting it.

    exec 3>&1 4>&2 >${gcloud_log_vm} 2>&1

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
    cd /home/${user}
    mkdir -p miniconda3
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda3/miniconda.sh
    bash miniconda3/miniconda.sh -b -u -p miniconda3
    rm -rf /home/${user}/miniconda3/miniconda.sh

    # log message
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "install miniconda3 done.", "PATH": "'$PATH'", "PWD": "'$PWD'"}' \
	--payload-type=json \
	--severity=INFO

    # set the path in order to find the conda command
    export PATH="/home/${user}/miniconda3/bin:/home/${user}/miniconda3/condabin:$PATH"

    # log message
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "set PATH manually done.", "PATH": "'$PATH'", "PWD": "'$PWD'"}' \
	--payload-type=json \
	--severity=INFO

    # install mamba
    source activate /home/${user}/miniconda3
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
    git clone https://ypradat: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@github.com/ypradat/FacetsTCGA.git /home/${user}/FacetsTCGA

    # log message
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "git clone FacetsTCGA done."}' \
	--payload-type=json \
	--severity=INFO

    # get resources and external
    cd /home/${user}/FacetsTCGA
    gsutil -m cp -r gs://facets_tcga/external .
    gsutil -m cp -r gs://facets_tcga/resources .

    # log message
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "gsutil cp external & resources done."}' \
	--payload-type=json \
	--severity=INFO

    # create env for running snakemake
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
    sudo chown -R ${user} /home/${user}

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

    # before deleting, check that the log was uploaded to the results folder.
    # if not, upload it to the failed folder
    gsutil -q stat gs://facets_tcga_results/logs/gcloud/startup_gcloud_vm_${batch_index}.log
    status=$?

    if [[ $status != 0 ]]; then
	gsutil cp ${gcloud_log_vm} gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_${batch_index}.log
    fi

    # delete instance
    gcloud compute instances delete $(hostname) --zone=${zone} --delete-disks=all --quiet
fi
