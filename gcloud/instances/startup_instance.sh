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
    # this case, we try to restart the startup script and the snakemake pipeline from where it left off. Of note, if the
    # VM was stopped in the middle of a dependency installation, the code may simply fail and the VM will have to be
    # recreated.

    exec 3>&1 4>&2 >>${gcloud_log_vm} 2>&1

    # log message
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "starting again a preempted job."}' \
	--payload-type=json \
	--severity=INFO

    now_date="$(date +'%d/%m/%Y')"
    now_time="$(date +'%T')"
    printf "\nStart date and time after preemption: %s %s\n" "$now_date" "$now_time"

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
fi

# Install all tools and dependencies required for the pipeline before downloading the pipeline and starting it.

# print full details about the instance
gcloud compute instances describe $(hostname) --zone=${zone}

# install git
if ! command -v git &> /dev/null
then
    sudo apt --assume-yes install git
fi

# install wget
if ! command -v wget &> /dev/null
then
    sudo apt --assume-yes install wget
fi


# install locales to avoid warning "/usr/bin/bash: warning: setlocale: LC_ALL: cannot change locale (en_US.UTF-8)"
if [[ ! $(sed -n '/^LANG="nb_NO.UTF-8"/p;q/p;q' /etc/default/locale) ]]
then
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
fi


if [[ ! -d "/home/${user}/TCGA_Facets" ]]
then
    # get the code
    git clone https://ypradat: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@github.com/ypradat/TCGA_Facets.git /home/${user}/TCGA_Facets

    # log message
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "git clone TCGA_Facets done."}' \
	--payload-type=json \
	--severity=INFO
fi

# get folders resources and external required for running the pipeline
cd /home/${user}/TCGA_Facets

if [[ ! -d "external" ]]
then
    gsutil -m cp -r gs://facets_tcga/external .

    # log message
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "gsutil cp external done."}' \
	--payload-type=json \
	--severity=INFO
fi

if [[ ! -d "resources" ]]
then
    gsutil -m cp -r gs://facets_tcga/resources .

    # log message
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "gsutil cp resources done."}' \
	--payload-type=json \
	--severity=INFO

    # in order to suppress warning message from snp-pileup
    touch resources/hg38_gatk/*.tbi
fi


# select samples
if [[ ! -f "config/samples.tsv" ]]
then
    awk -F '\t' -v i="${batch_index}" 'NR==1; {if($(NF)==i) print $0}' config/samples.all.tsv > config/samples.tsv
    awk -F '\t' -v i="${batch_index}" 'NR==1; {if($(NF)==i) print $0}' config/tumor_normal_pairs.all.tsv > config/tumor_normal_pairs.tsv

    # log message
    gcloud logging write ${gcloud_log_name} \
	'{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "samples selection done."}' \
	--payload-type=json \
	--severity=INFO
fi


# install conda and mamba
if [[ ! -d "/home/${user}/miniconda3" ]]
then
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
fi

# set the path in order to find the conda command
export PATH="/home/${user}/miniconda3/bin:/home/${user}/miniconda3/condabin:$PATH"
source activate /home/${user}/miniconda3

# log message
gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "set PATH manually done.", "PATH": "'$PATH'", "PWD": "'$PWD'"}' \
    --payload-type=json \
    --severity=INFO

# install mamba
if ! command -v mamba &> /dev/null
then
    conda install -y -c conda-forge mamba
fi

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

# create env for running snakemake
if [[ ! -d "${snakemake_env_dir}" ]]
then
    mamba env create --prefix ${snakemake_env_dir} -f /home/${user}/TCGA_Facets/workflow/envs/snakemake.yaml

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
fi

# activate snakemake and run
source activate ${snakemake_env_dir}

# set permissions to user
sudo chown -R ${user} /home/${user}

# if the pipeline has already failed once for this batch, reduce the load because very often the
# failure occurs due to memory issues.
gsutil ls gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_first_${batch_index}.log &> /dev/null
status_failed_first=$?

# if the pipeline has already failed twice for this batch, reduce to only job at a time to
# avert memory issues.
gsutil ls gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_second_${batch_index}.log &> /dev/null
status_failed_second=$?

# if the pipeline has already failed three times for this batch due to memory usage, reduce to only job at a time to
# avert memory issues.
gsutil ls gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_third_oom_${batch_index}.log &> /dev/null
status_failed_third_oom=$?

if [[ ${status_failed_second} != 0 ]] && [[ ${status_failed_first} != 0 ]] && [[ ${status_failed_third_oom} != 0 ]]; then
    # first time this batch is run, try to run the maximum number of jobs in parallel
    load=115
    jobs=50
elif [[ ${status_failed_first} == 0 ]]; then
    # second time this batch is run, reduce the maximum number of jobs that can run in parallel
    load=65
    jobs=50

    # log message
    gcloud logging write ${gcloud_log_name} \
        '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "this batch has already failed once, reducing the load to '${load}'."}' \
        --payload-type=json \
        --severity=INFO

    # removing existing failed log
    gsutil rm gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_first_${batch_index}.log
elif [[ ${status_failed_second} == 0 ]] ; then
    # third time this batch is run, reduce the maximum number of jobs that can run in parallel to 1
    load=65
    jobs=1

    # log message
    gcloud logging write ${gcloud_log_name} \
        '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "this batch has already failed twice, reducing the number of jobs to '${jobs}'."}' \
        --payload-type=json \
        --severity=INFO

    # removing existing failed log
    gsutil rm gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_second_${batch_index}.log
elif [[ ${status_failed_third_oom} == 0 ]] ; then
    # fourth time this batch is run, reduce the maximum number of jobs that can run in parallel to 1
    load=115
    jobs=50

    # log message
    gcloud logging write ${gcloud_log_name} \
        '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "this batch has already failed three times, running on a larger VM with jobs at '${jobs}'."}' \
        --payload-type=json \
        --severity=INFO

    # removing existing failed log
    gsutil rm gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_third_oom_${batch_index}.log
fi

# run the pipeline, rerunning incomplete jobs
# log message
cd /home/${user}/TCGA_Facets

gcloud logging write ${gcloud_log_name} \
    '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "started pipeline."}' \
    --payload-type=json \
    --severity=INFO

snakemake -s workflow/Snakefile --profile ./profile --resources load=${load} --jobs ${jobs} -f --unlock
snakemake -s workflow/Snakefile --profile ./profile --resources load=${load} --jobs ${jobs} -f --rerun-incomplete

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
gsutil ls gs://facets_tcga_results/logs/gcloud/startup_gcloud_vm_${batch_index}.log &> /dev/null
status_failed_cur=$?

if [[ ${status_failed_cur} != 0 ]]; then
    if [[ ${status_failed_first} != 0 ]] && [[ ${status_failed_second} != 0 ]] && [[ ${status_failed_third_oom} != 0 ]]; then
        # there is no failure log from a previous run of this batch

        # log message
        gcloud logging write ${gcloud_log_name} \
            '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "pipeline failed for the first time."}' \
            --payload-type=json \
            --severity=WARNING

        gsutil cp ${gcloud_log_vm} gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_first_${batch_index}.log
    elif [[ ${status_failed_first} == 0 ]]; then
        # there was a failure log after a first run of this batch

        # log message
        gcloud logging write ${gcloud_log_name} \
            '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "pipeline failed for the second time."}' \
            --payload-type=json \
            --severity=WARNING

        gsutil cp ${gcloud_log_vm} gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_second_${batch_index}.log
    elif [[ ${status_failed_second} == 0 ]]; then
        # there is a failure log after a second run of this batch

        # identify if the job failed because it ran out-of-memory. if yes, make one last try with a larger memory VM
        grep -rn "Killed" ${gcloud_log_vm}
        status_oom=$?

        if [[ ${status_oom} == 0 ]]; then
            # log message
            gcloud logging write ${gcloud_log_name} \
                '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "pipeline failed for the third time with oom error."}' \
                --payload-type=json \
                --severity=WARNING

            gsutil cp ${gcloud_log_vm} gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_third_oom_${batch_index}.log
        else
            gsutil cp ${gcloud_log_vm} gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_third_oth_${batch_index}.log

            # log message
            gcloud logging write ${gcloud_log_name} \
                '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "pipeline failed for the third time with not oom error."}' \
                --payload-type=json \
                --severity=ERROR

            # upload whatever was done
            python -u gcloud/buckets/populate_results_gs_bucket.py \
                --bucket_gs_uri "gs://facets_tcga_results"
        fi
    elif [[ ${status_failed_third_oom} == 0 ]]; then
        # there is a failure log after a third run of this batch

        # log message
        gcloud logging write ${gcloud_log_name} \
            '{"instance-id": "'${instance_id}'", "hostname": "'$(hostname)'", "message": "pipeline failed for the fourth time."}' \
            --payload-type=json \
            --severity=ERROR

        gsutil cp ${gcloud_log_vm} gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_fourth_${batch_index}.log

        # upload whatever was done
        python -u gcloud/buckets/populate_results_gs_bucket.py \
            --bucket_gs_uri "gs://facets_tcga_results"
    fi 
fi

# delete instance
gcloud compute instances delete $(hostname) --zone=${zone} --delete-disks=all --quiet
