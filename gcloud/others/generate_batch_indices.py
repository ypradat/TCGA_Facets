# -*- coding: utf-8 -*-
"""
@created: Oct 14 2022
@modified: Oct 19 2022
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

def get_batch_index(x, prefix_1="startup_gcloud_vm_", prefix_2="", suffix=".log"):
    prefix = "%s%s" % (prefix_1, prefix_2)
    x_split_prefix = x.split(prefix)
    if len(x_split_prefix) == 1:
        return None
    else:
        x = x_split_prefix[1]
        x_split_suffix = x.split(suffix)
        return int(x_split_suffix[0])


def remove_none_entries(l):
    return [e for e in l if e is not None]


def main(args):
    # load list of all possible batches
    df_sam = pd.read_table(args.samples_table)
    batch_list_samples = sorted(list(set(df_sam["Batch"].tolist())))

    # select indices according to user specifications
    batch_list_samples = [x for x in batch_list_samples if x >= args.batch_min]
    if args.batch_max != -1:
        batch_list_samples = [x for x in batch_list_samples if x <= args.batch_max]

    # remove batches that were already processed
    cmd_gsutil = "gsutil ls %s" % args.logs_uri
    cmd_output = subprocess.run(cmd_gsutil, shell=True, capture_output=True)
    stdout = cmd_output.stdout.decode("utf-8")
    batches_log_names = [os.path.basename(file) for file in stdout.split()]
    batches_processed = [get_batch_index(log) for log in batches_log_names]
    batches_processed = remove_none_entries(batches_processed)
    batch_list = sorted(list(set(batch_list_samples).difference(set(batches_processed))))

    # remove batches that already failed three times with another error than oom
    cmd_gsutil = "gsutil ls %s" % args.logs_failed_uri
    cmd_output = subprocess.run(cmd_gsutil, shell=True, capture_output=True)
    stdout = cmd_output.stdout.decode("utf-8")
    batches_log_names = [os.path.basename(file) for file in stdout.split()]
    batches_failed_third = [get_batch_index(log, prefix_2="third_error") for log in batches_log_names]
    batches_failed_third = remove_none_entries(batches_failed_third)
    batch_list = sorted(list(set(batch_list).difference(set(batches_failed_third))))

    # remove batches that already failed four times
    cmd_gsutil = "gsutil ls %s" % args.logs_failed_uri
    cmd_output = subprocess.run(cmd_gsutil, shell=True, capture_output=True)
    stdout = cmd_output.stdout.decode("utf-8")
    batches_log_names = [os.path.basename(file) for file in stdout.split()]
    batches_failed_fourth = [get_batch_index(log, prefix_2="fourth_") for log in batches_log_names]
    batches_failed_fourth = remove_none_entries(batches_failed_fourth)
    batch_list = sorted(list(set(batch_list).difference(set(batches_failed_fourth))))

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
    parser.add_argument('--logs_failed_uri', type=str, help='Path to directory of VM logs.',
                        default="gs://facets_tcga_results/logs/gcloud_failed")
    parser.add_argument('--batch_min', type=int, help='Minimum index of batches to be processed.',
                        default=1)
    parser.add_argument('--batch_max', type=int,
                        help='Maximum index of batches to be processed. Use -1 for no maximum limit.',
                        default=-1)
    parser.add_argument('--batch_list', type=str, help='Path to output list of batch indices.',
                        default="gcloud/batch_indices.txt")
    args = parser.parse_args()

    main(args)
