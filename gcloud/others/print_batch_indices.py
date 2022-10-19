# -*- coding: utf-8 -*-
"""
@created: Oct 19 2022
@modified: Oct 19 2022
@author: Yoann Pradat

    CentraleSupelec
    MICS laboratory
    9 rue Juliot Curie, Gif-Sur-Yvette, 91190 France

    Institut Gustave Roussy
    Prism Center
    114 rue Edouard Vaillant, Villejuif, 94800 France

Print to stdout a list of batch indices according to user specifications.
"""

import argparse
import pandas as pd
import os
import subprocess

# functions ============================================================================================================

def get_batch_index(x, prefix="startup_gcloud_vm_first", suffix=".log"):
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
    # list batches
    cmd_gsutil = "gsutil ls %s" % args.logs_uri
    cmd_output = subprocess.run(cmd_gsutil, shell=True, capture_output=True)
    stdout = cmd_output.stdout.decode("utf-8")
    batches_log_names = [os.path.basename(file) for file in stdout.split()]
    batch_list = [get_batch_index(log, prefix=args.prefix) for log in batches_log_names]
    batch_list = remove_none_entries(batch_list)
    batch_list = sorted(batch_list)

    # select indices according to user specifications
    batch_list = [x for x in batch_list if x >= args.batch_min]
    if args.batch_max != -1:
        batch_list = [x for x in batch_list if x <= args.batch_max]

    # print
    for batch in batch_list:
        print(batch)


# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Print to stdout a list of batch indices.")
    parser.add_argument('--logs_uri', type=str, help='Path to directory of VM logs.',
                        default="gs://facets_tcga_results/logs/gcloud_failed")
    parser.add_argument('--prefix', type=str, help='Prefix for selecting logs.',
                        default="startup_gcloud_vm_first_")
    parser.add_argument('--batch_min', type=int, help='Minimum index of batches to be printed.',
                        default=1)
    parser.add_argument('--batch_max', type=int,
                        help='Maximum index of batches to be printed.',
                        default=-1)
    args = parser.parse_args()

    main(args)
