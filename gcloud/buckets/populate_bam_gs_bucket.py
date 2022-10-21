# -*- coding: utf-8 -*-
"""
@created: Oct 11 2022
@modified: Oct 21 2022
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
    samples = df_sam.loc[df_sam["Batch"]==args.batch_index, "Sample_Id"].drop_duplicates().tolist()
    if len(samples)==0:
        print("-WARNING: batch %d not found in the table %s:" % (args.batch_index, args.samples_table))

    # copy to bucket and rename
    for sample in samples:
        # copy
        bam_gs_uri = df_sam.loc[df_sam["Sample_Id"]==sample, "File_Name_Key"].item()
        bai_gs_uri = df_sam.loc[df_sam["Sample_Id"]==sample, "Index_File_Name_Key"].item()
        bam_name = df_sam.loc[df_sam["Sample_Id"]==sample, "File_Name"].item()
        bai_name = df_sam.loc[df_sam["Sample_Id"]==sample, "Index_File_Name"].item()
        bam_name_new = "%s.bam" % sample
        bai_name_new = "%s.bai" % sample

        # before copying, check if the file already exists
        cmd_bam_ls = "gsutil ls %s/%s" % (args.bucket_gs_uri, bam_name_new)
        cm_bam_ls_out = subprocess.run(cmd_bam_ls, shell=True, capture_output=True)

        # returncode 0 means that the file already exists
        if cm_bam_ls_out.returncode==0:
            print("-%s/%s already exists!" % (args.bucket_gs_uri, bam_name_new))
        else
            cmd_bam_cp = "gsutil cp %s %s" % (bam_gs_uri, args.bucket_gs_uri)
            subprocess.run(cmd_bam_cp, shell=True)
            print("-copied bam file %s to bucket" % bam_name)
            cmd_bam_mv = "gsutil mv %s/%s %s/%s" % (args.bucket_gs_uri, bam_name, args.bucket_gs_uri, bam_name_new)
            subprocess.run(cmd_bam_mv, shell=True)
            print("-renamed bam file %s to %s" % (bam_name, bam_name_new))

        # before copying, check if the file already exists
        cmd_bai_ls = "gsutil ls %s/%s" % (args.bucket_gs_uri, bai_name_new)
        cm_bai_ls_out = subprocess.run(cmd_bai_ls, shell=True, capture_output=True)

        # returncode 0 means that the file already exists
        if cm_bai_ls_out.returncode==0:
            print("-%s/%s already exists!" % (args.bucket_gs_uri, bai_name_new))
        else
            cmd_bai_cp = "gsutil cp %s %s" % (bai_gs_uri, args.bucket_gs_uri)
            subprocess.run(cmd_bai_cp, shell=True)
            print("-copied bai file %s to bucket" % bai_name)
            cmd_bai_mv = "gsutil mv %s/%s %s/%s" % (args.bucket_gs_uri, bai_name, args.bucket_gs_uri, bai_name_new)
            subprocess.run(cmd_bai_mv, shell=True)
            print("-renamed bai file %s to %s" % (bai_name, bai_name_new))

# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add all BAM files from selected batch to gcloud bucket.")
    parser.add_argument('--samples_table', type=str, help='Path to input table.', default="config/samples.all.tsv")
    parser.add_argument('--bucket_gs_uri', type=str, help='Google cloud storage URI to bucket.',
                        default="gs://tcga_wxs_bam")
    parser.add_argument('--batch_index', type=int, help='Index of the batch.')
    args = parser.parse_args()

    main(args)
