# -*- coding: utf-8 -*-
"""
@created: Oct 11 2022
@modified: Oct 11 2022
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

# functions ============================================================================================================

def main(args):
    # rules from which to copy
    supfolders_vm = ["results", "workflow/logs", "workflow/benchmarks"]
    supfolders_gs = ["%s/%s" % (args.bucket_gs_uri, x) for x in ["results", "logs", "benchmarks"]]
    midfolders = ["calling", "annotation"]
    subfolders = ["somatic_cnv_bed", "somatic_cnv_calls", "somatic_cnv_chr_arm", "somatic_cnv_facets",
                  "somatic_cnv_sum", "somatic_cnv_table", "somatic_cna_civic", "somatic_cna_civic_filter",
                  "somatic_cna_civic_preprocess",  "somatic_cna_oncokb",  "somatic_cna_oncokb_filter",
                  "somatic_cna_oncokb_preprocess"]

    for supfolder_vm, supfolder_gs in zip(supfolders_vm, supfolders_gs):
        if os.path.exists(supfolder_vm):
            for midfolder in midfolders:
                folder_vm = os.path.join(supfolder_vm, midfolder)
                if os.path.exists(folder_vm):
                    for subfolder in subfolders:
                        folder_vm = os.path.join(supfolder_vm, midfolder, subfolder)
                        folder_gs = os.path.join(supfolder_gs, midfolder, subfolder)
                        if os.path.exists(folder_vm):
                            files_vm = os.listdir(folder_vm)
                            filepaths_vm = [os.path.join(folder_vm, file_vm) for file_vm in files_vm]
                            filepaths_gs = [os.path.join(folder_gs, file_vm) for file_vm in files_vm]
                            for filepath_vm, filepath_gs in zip(filepaths_vm, filepaths_gs):
                                cmd_cp = "gsutil cp %s %s" % (filepath_vm, filepath_gs)
                                subprocess.run(cmd_cp, shell=True)
                                print("-copied file %s to bucket" % filepath_vm)

# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add Tumor_Sample and Normal_Sample fields.")
    parser.add_argument('--bucket_gs_uri', type=str, help='Google cloud storage URI to bucket.',
                        default="gs://facets_tcga_results")
    args = parser.parse_args()

    for arg in vars(args):
        print("%s: %s" % (arg, getattr(args, arg)))
    print("\n")

    main(args)
