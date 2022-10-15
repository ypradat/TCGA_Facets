# -*- coding: utf-8 -*-
"""
@created: Oct 12 2022
@modified: Oct 13 2022
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
    if args.batch_index is not None:
        samples = df_sam.loc[df_sam["Batch"]==args.batch_index, "Sample_Id"].drop_duplicates().tolist()
    else:
        samples = df_sam["Sample_Id"].drop_duplicates().tolist()

    if len(samples)==0:
        print("-WARNING: batch %d not found in the table %s:" % (args.batch_index, args.samples_table))

    # copy to bucket and rename
    for sample in samples:
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
    parser = argparse.ArgumentParser(description="Remove all BAM files from selected batch from gcloud bucket.")
    parser.add_argument('--samples_table', type=str, help='Path to input table.', default="config/samples.tsv")
    parser.add_argument('--bucket_gs_uri', type=str, help='Google cloud storage URI to bucket.',
                        default="gs://tcga_wxs_bam")
    parser.add_argument('--batch_index', type=int, help='Index of the batch.', default=None)
    args = parser.parse_args()

    for arg in vars(args):
        print("%s: %s" % (arg, getattr(args, arg)))
    print("\n")

    main(args)
