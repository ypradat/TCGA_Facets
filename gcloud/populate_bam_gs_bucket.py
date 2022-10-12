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

Copy TCGA WXS BAM to google cloud bucket.
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
        # copy
        bam_gs_uri = df_sam.loc[df_sam["Sample_Id"]==sample, "File_Name_Key"].item()
        bai_gs_uri = df_sam.loc[df_sam["Sample_Id"]==sample, "Index_File_Name_Key"].item()
        bam_name = df_sam.loc[df_sam["Sample_Id"]==sample, "File_Name"].item()
        bai_name = df_sam.loc[df_sam["Sample_Id"]==sample, "Index_File_Name"].item()

        cmd_bam_cp = "gsutil cp %s %s" % (bam_gs_uri, args.bucket_gs_uri)
        cmd_bai_cp = "gsutil cp %s %s" % (bai_gs_uri, args.bucket_gs_uri)

        subprocess.run(cmd_bam_cp, shell=True)
        print("-copied bam file %s to bucket" % bam_name)

        subprocess.run(cmd_bai_cp, shell=True)
        print("-copied bam file %s to bucket" % bai_name)

        # rename
        bam_name_new = "%s.bam" % sample
        bai_name_new = "%s.bai" % sample

        cmd_bam_mv = "gsutil mv %s/%s %s/%s" % (args.bucket_gs_uri, bam_name, args.bucket_gs_uri, bam_name_new)
        cmd_bai_mv = "gsutil mv %s/%s %s/%s" % (args.bucket_gs_uri, bai_name, args.bucket_gs_uri, bai_name_new)

        subprocess.run(cmd_bam_mv, shell=True)
        print("-renamed bam file %s to %s" % (bam_name, bam_name_new))

        subprocess.run(cmd_bai_mv, shell=True)
        print("-renamed bai file %s to %s" % (bai_name, bai_name_new))

# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add Tumor_Sample and Normal_Sample fields.")
    parser.add_argument('--samples_table', type=str, help='Path to input table.', default="config/samples.tsv")
    parser.add_argument('--bucket_gs_uri', type=str, help='Google cloud storage URI to bucket.',
                        default="gs://tcga_wxs_bam")
    parser.add_argument('--samples', type=str, nargs="+", help='Samples to be added to the bucket.',
                        default=["TCGA-02-0003-01A-01D-1490-08", "TCGA-02-0003-10A-01D-1490-08"])
    args = parser.parse_args()

    for arg in vars(args):
        print("%s: %s" % (arg, getattr(args, arg)))
    print("\n")

    main(args)
