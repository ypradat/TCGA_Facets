#!/bin/bash

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

# change directory
cd /home/ypradat

# install conda and mamba
mkdir -p miniconda3
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda3/miniconda.sh
bash miniconda3/miniconda.sh -b -u -p miniconda3
rm -rf miniconda3/miniconda.sh

cat <<'EOF' >>.bashrc
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

cat <<'EOF'>>.condarc
auto_activate_base: false
channel_priority: strict
EOF

source .bashrc

# # install mamba
# sudo chown -R ypradat /home/ypradat/miniconda3
# sudo chmod -R +x /home/ypradat/miniconda3
# conda activate /home/ypradat/miniconda3
# conda install -y -c conda-forge mamba

# get the code
git clone https://ypradat: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@github.com/ypradat/FacetsTCGA.git
cd FacetsTCGA

# get resources and external
gsutil -m cp -r gs://facets_tcga/external .
gsutil -m cp -r gs://facets_tcga/resources .

# prepare results folder
mkdir -p results/mapping

# # prepare for running snakemake
# mamba env create --prefix /home/ypradat/miniconda3/envs/snakemake -f workflow/envs/snakemake.yaml
# 
# # activate snakemake and run
# conda activate /home/ypradat/miniconda3/envs/snakemake

# select sample
BATCH_INDEX=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/batch_index -H "Metadata-Flavor: Google")

# select samples
awk -F '\t' -v i="${BATCH_INDEX}" 'NR==1; {if($(NF)==i) print $0}' config/samples.all.tsv > config/samples.tsv
awk -F '\t' -v i="${BATCH_INDEX}" 'NR==1; {if($(NF)==i) print $0}' config/tumor_normal_pairs.all.tsv > config/tumor_normal_pairs.tsv

# run the command
# snakemake -s workflow/Snakefile --profile ./profile --resources load=100 -f
