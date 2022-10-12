# -*- coding: utf-8 -*-
"""
@created: Oct 12 2022
@modified: Oct 12 2022
@author: Yoann Pradat

    CentraleSupelec
    MICS laboratory
    9 rue Juliot Curie, Gif-Sur-Yvette, 91190 France

    Institut Gustave Roussy
    Prism Center
    114 rue Edouard Vaillant, Villejuif, 94800 France

Remove TCGA WXS BAM from google cloud bucket.
"""

import argparse
import os
import pandas as pd
import subprocess

# functions ============================================================================================================

def main(args):
    # load table
    df_sam = pd.read_table(args.samples_table)

    # check that samples requested are in the table
    samples = args.samples
    samples_missing = list(set(samples).difference(set(df_sam["Sample_Id"])))
    if len(samples_missing)>0:
        print("-WARNING: the following samples are missing from the table %s:" % args.samples_table)
        print("\t" + "\n\t".join(samples_missing))
    samples_found = [x for x in samples if x not in samples_missing]

    # copy to bucket and rename
    for sample in samples_found:
        # get bam uris
        bam_name = "%s.bam" % sample
        bai_name = "%s.bai" % sample
        bam_gs_uri = os.path.join(args.bucket_gs_uri, bam_name)
        bai_gs_uri = os.path.join(args.bucket_gs_uri, bai_name)

        cmd_bam_rm = "gsutil rm %s" % (bam_gs_uri)
        cmd_bai_rm = "gsutil rm %s" % (bai_gs_uri)

        subprocess.run(cmd_bam_rm, shell=True)
        print("-deleted bam file %s from bucket" % bam_name)

        subprocess.run(cmd_bai_rm, shell=True)
        print("-deleted bam file %s from bucket" % bai_name)

# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add Tumor_Sample and Normal_Sample fields.")
    parser.add_argument('--samples_table', type=str, help='Path to input table.', default="config/samples.tsv")
    parser.add_argument('--bucket_gs_uri', type=str, help='Google cloud storage URI to bucket.',
                        default="gs://tcga_wxs_bam")
    parser.add_argument('--samples', type=str, nargs="+", help='Samples to be added to the bucket.',
                        default=["TCGA-05-4244-01A-01D-1105-08", "TCGA-05-4244-10A-01D-1105-08",
                                 "TCGA-05-4415-01A-22D-1855-08", "TCGA-05-4415-10A-01D-1855-08"])
    args = parser.parse_args()

    for arg in vars(args):
        print("%s: %s" % (arg, getattr(args, arg)))
    print("\n")

    main(args)
