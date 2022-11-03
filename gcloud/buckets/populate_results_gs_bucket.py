# -*- coding: utf-8 -*-
"""
@created: Oct 11 2022
@modified: Nov 03 2022
@author: Yoann Pradat

    CentraleSupelec
    MICS laboratory
    9 rue Juliot Curie, Gif-Sur-Yvette, 91190 France

    Institut Gustave Roussy
    Prism Center
    114 rue Edouard Vaillant, Villejuif, 94800 France

Copy results, logs, and benchmarks to a dedicated bucket.
"""

import argparse
import os
import pandas as pd
import subprocess
from multiprocessing import Pool
from tqdm import tqdm

# functions ============================================================================================================

def main(args):
    # rules from which to copy
    supfolders_vm = ["results", "workflow/logs", "workflow/benchmarks"]
    supfolders_gs = ["%s/%s" % (args.bucket_gs_uri, x) for x in ["results", "logs", "benchmarks"]]
    midfolders = ["calling", "annotation"]

    subfolders = ["somatic_cnv_process_vcf", "somatic_cnv_chr_arm", "somatic_cnv_sum", "somatic_cnv_table",
                  "somatic_cnv_gene_calls_unfiltered", "somatic_cnv_gene_calls_filtered",
                  "somatic_cna_civic", "somatic_cna_oncokb"]

    if args.start_from in ["download_bam", "get_snp_pileup", "somatic_cnv_facets"]:
        subfolders = ["somatic_cnv_facets"] + subfolders

    cmds = []
    for supfolder_vm, supfolder_gs in zip(supfolders_vm, supfolders_gs):
        if os.path.exists(supfolder_vm):
            for midfolder in midfolders:
                folder_vm = os.path.join(supfolder_vm, midfolder)
                if os.path.exists(folder_vm):
                    for subfolder in subfolders:
                        folder_vm = os.path.join(supfolder_vm, midfolder, subfolder)
                        folder_gs = os.path.join(supfolder_gs, midfolder, subfolder)
                        if os.path.exists(folder_vm):
                            cmd = "gsutil -m rsync -r %s %s" % (folder_vm, folder_gs)
                            cmds.append(cmd)

    # upload VM log
    home = "/home/ypradat"
    vm_log = [x for x in os.listdir(home) if x.startswith("startup_gcloud_vm")][0]
    filepath_vm = os.path.join(home, vm_log)
    filepath_gs = os.path.join(args.bucket_gs_uri, "logs/gcloud", vm_log)
    cmd = "gsutil cp %s %s" % (filepath_vm, filepath_gs)
    cmds.append(cmd)

    # run all commands
    for cmd in cmds:
        subprocess.run(cmd, shell=True)


# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload results to bucket.")
    parser.add_argument('--bucket_gs_uri', type=str, help='Google cloud storage URI to bucket.',
                        default="gs://facets_tcga_results")
    parser.add_argument('--start_from', type=str, help='Rule name from which the pipeline started.',
                        default="download_bam")
    args = parser.parse_args()

    for arg in vars(args):
        print("%s: %s" % (arg, getattr(args, arg)))
    print("\n")

    main(args)
