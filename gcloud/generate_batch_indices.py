# -*- coding: utf-8 -*-
"""
@created: Oct 14 2022
@modified: Oct 14 2022
@author: Yoann Pradat

    CentraleSupelec
    MICS laboratory
    9 rue Juliot Curie, Gif-Sur-Yvette, 91190 France

    Institut Gustave Roussy
    Prism Center
    114 rue Edouard Vaillant, Villejuif, 94800 France

Save list of batch indices to be processed in text file.
"""

import argparse
import pandas as pd
import os
import subprocess

# functions ============================================================================================================

def main(args):
    df_sam = pd.read_table(args.samples_table)
    batches_list = sorted(list(set(df_sam["Batch"].tolist())))

    # select indices according to user specifications
    batches_list = [x for x in batches_list if x >= args.batch_min]
    if args.batch_max != -1:
        batches_list = [x for x in batches_list if x <= args.batch_max]

    # remove batches already processed
    cmd_gsutil = "gsutil ls %s" % args.logs_uri
    cmd_output = subprocess.run(cmd_gsutil, shell=True, capture_output=True)
    stdout = cmd_output.stdout.decode("utf-8")
    batches_log_names = [os.path.basename(file) for file in stdout.split()]
    batches_processed = [int(log.split("startup_gcloud_vm_")[1].split(".log")[0]) for log in batches_log_names]
    batch_list = sorted(list(set(batches_list).difference(set(batches_processed))))

    # save
    with open(args.batch_list, "w") as file:
        for batch_index in batch_list:
            file.write("%d\n" % batch_index)


# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Save list of batch indices to be processed in text file.")
    parser.add_argument('--samples_table', type=str, help='Path to samples table.',
                        default="config/samples.all.tsv")
    parser.add_argument('--logs_uri', type=str, help='Path to directory of VM logs.',
                        default="gs://facets_tcga_results/logs/gcloud")
    parser.add_argument('--batch_min', type=int, help='Minimum index of batches to be processed.',
                        default=1)
    parser.add_argument('--batch_max', type=int,
                        help='Maximum index of batches to be processed. Use -1 for no maximum limit.',
                        default=10)
    parser.add_argument('--batch_list', type=str, help='Path to output list of batch indices.',
                        default="gcloud/batch_indices.txt")
    args = parser.parse_args()

    for arg in vars(args):
        print("%s: %s" % (arg, getattr(args, arg)))
    print("\n")

    main(args)
