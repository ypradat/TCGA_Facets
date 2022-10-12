#!/bin/bash

#TODO: provide sample(s) names in order to subselect the table config/samples.tsv in the VM.
#TODO: solve authentification issues when uploading to the bucket gs://facets_tcga_results
#TODO: fine-tune memory usage according to BAM sizes

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
