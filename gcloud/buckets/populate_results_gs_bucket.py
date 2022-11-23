# -*- coding: utf-8 -*-
"""
@created: Oct 11 2022
@modified: Nov 23 2022
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
    if not args.vm_log:
        # rules from which to copy
        supfolders_vm = ["results", "workflow/logs", "workflow/benchmarks"]
        supfolders_gs = ["%s/%s" % (args.bucket_gs_uri, x) for x in ["results", "logs", "benchmarks"]]
        midfolders = ["mapping", "calling", "annotation"]

        subfolders = ["somatic_cnv_process_vcf", "somatic_cnv_chr_arm", "somatic_cnv_sum", "somatic_cnv_table",
                      "somatic_cnv_gene_calls", "somatic_cna_civic", "somatic_cna_oncokb"]

        if args.start_from in ["download_bam"]:
            subfolders = ["download_bam"] + subfolders

        if args.start_from in ["download_bam", "get_snp_pileup"]:
            subfolders = ["get_snp_pileup"] + subfolders

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
                                if args.tsample=="NA" and args.nsample=="NA":
                                    # if both tsample and nsample are NA, upload the whole folder
                                    cmd = "gsutil -m rsync -r %s %s" % (folder_vm, folder_gs)
                                    cmds.append(cmd)
                                else:
                                    # otherwise, upload only the files corresponding to this pair
                                    pair = "%s_vs_%s" % (args.tsample, args.nsample)
                                    pair_files = [x for x in os.listdir(folder_vm) if x.startswith(pair)]
                                    for pair_file in pair_files:
                                        pair_file_vm = os.path.join(folder_vm, pair_file)
                                        pair_file_gs = os.path.join(folder_gs, pair_file)
                                        cmd = "gsutil cp %s %s" % (pair_file_vm, pair_file_gs)
                                        cmds.append(cmd)

        # run all commands
        for cmd in cmds:
            subprocess.run(cmd, shell=True)

    else:
        # upload VM log
        home = "/home/ypradat"
        vm_log = [x for x in os.listdir(home) if x.startswith("startup_gcloud_vm")][0]
        filepath_vm = os.path.join(home, vm_log)
        filepath_gs = os.path.join(args.bucket_gs_uri, "logs/gcloud", vm_log)
        cmd = "gsutil cp %s %s" % (filepath_vm, filepath_gs)
        subprocess.run(cmd, shell=True)

# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload results to bucket.")
    parser.add_argument('--bucket_gs_uri', type=str, help='Google cloud storage URI to bucket.',
                        default="gs://facets_tcga_results")
    parser.add_argument('--start_from', type=str, help='Rule name from which the pipeline started.',
                        default="download_bam")
    parser.add_argument('--tsample', type=str, help='Name of tumor sample.',
                        default="NA")
    parser.add_argument('--nsample', type=str, help='Name of normal sample.',
                        default="NA")
    parser.add_argument('--vm_log', action="store_true", help='If used, only the log of the VM is uploaded.',
                        default=False)
    args = parser.parse_args()

    for arg in vars(args):
        print("%s: %s" % (arg, getattr(args, arg)))
    print("\n")

    main(args)
