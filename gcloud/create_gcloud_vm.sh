#!/bin/bash

while getopts ":b:" opt; do
    case $opt in
	i) batch_index="$OPTARG"
	    ;;
	\?) echo "Invalid option -$OPTARG" >&2
	    exit 1
	    ;;
    esac

    case $OPTARG in
	-*) echo "Option $opt needs a valid argument"
	    exit 1
	    ;;
    esac
done

# Populate the bucket tcga_wxs_bam
python -u workflow/scripts/populate_bam_gs_bucket.py \
   --samples_table config/samples.tsv \ 
   --bucket_gs_uri "gs://tcga_wxs_bam" \ 
   --batch_index ${batch_index}

# Create the instance and run the pipeline
gcloud compute instances create facets-tcga-${index} \
    --project=isb-cgc-external-001 \
    --zone=us-central1-a \
    --machine-type=e2-highmem-8 \
    --network-interface=network-tier=PREMIUM,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=482716779852-compute@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
    --enable-display-device \
    --tags=https-server \
    --create-disk=auto-delete=yes,boot=yes,device-name=facets-tcga,image=projects/debian-cloud/global/images/debian-11-bullseye-v20220920,mode=rw,size=200,type=projects/isb-cgc-external-001/zones/us-central1-a/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --reservation-affinity=any \
    --metadata=startup-script='#! /bin/bash

    # install git
    sudo apt install git

    # install wget
    sudo apt install wget

    # install locales to avoid warning "/usr/bin/bash: warning: setlocale: LC_ALL: cannot change locale (en_US.UTF-8)"
    sudo apt install locales
    sudo update-locale "LANG=en_US.UTF-8"
    sudo locale-gen --purge "en_US.UTF-8"
    sudo dpkg-reconfigure --frontend noninteractive locales

    # install conda and mamba
    mkdir -p ~/miniconda3
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda3/miniconda.sh
    bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
    rm -rf ~/miniconda3/miniconda.sh
    ~/miniconda3/bin/conda init bash
    source ~/.bashrc
    conda install -y -c conda-forge mamba
    conda config --set channel_priority strict

    # get the code
    git clone https://ypradat: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@github.com/ypradat/FacetsTCGA.git
    cd FacetsTCGA

    # get resources and external
    gsutil cp -r gs://facets_tcga/external .
    gsutil cp -r gs://facets_tcga/resources .

    # prepare results folder
    mkdir -p results/mapping

    # prepare for running snakemake
    mamba env create -f workflow/envs/snakemake.yaml

    # activate snakemake and run
    conda activate snakemake

    # run the command
    snakemake -s workflow/Snakefile --profile ./profile -f
    '

# Clean the bucket tcga_wxs_bam
python -u workflow/scripts/depopulate_bam_gs_bucket.py \
   --samples_table config/samples.tsv \ 
   --bucket_gs_uri "gs://tcga_wxs_bam" \ 
   --batch_index ${batch_index}
