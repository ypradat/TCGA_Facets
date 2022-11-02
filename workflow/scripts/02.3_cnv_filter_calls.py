# -*- coding: utf-8 -*-
"""
@created: Apr 28 2022
@modified: Oct 03 2022
@author: Yoann Pradat

    CentraleSupelec
    MICS laboratory
    9 rue Juliot Curie, Gif-Sur-Yvette, 91190 France

    Institut Gustave Roussy
    Prism Center
    114 rue Edouard Vaillant, Villejuif, 94800 France

Convert a table of CNV calls in bed format to a table of copy-number filtered calls.
"""

import argparse
import gzip
import numpy as np
import os
import pandas as pd

# functions ============================================================================================================

def convert_num_to_str(x):
    try:
        y = "%d" % int(x)
    except:
        try:
            y = "%f" % float(x)
            if y=="nan":
                y = x
        except:
            y = x

    return y


def main(args):
    # load data
    df_bed = pd.read_table(args.input_bed)

    # drop genes with overlap 0
    mask = df_bed["overlap"]!=0
    df_bed = df_bed.loc[mask]
    print("-INFO: dropped %d/%d lines (~ genes) with 0 overlap" % (sum(~mask), len(mask)))

    # drop events covering more than X Mb
    mask = df_bed["svlen"] < args.threshold*1e6
    df_bed = df_bed.loc[mask]
    print("-INFO: dropped %d/%d lines (~ genes) from SV longer than %s Mb" % (sum(~mask), len(mask), args.threshold))

    # for genes with different copy-number, select the copy-number from the smallest SV (prioritize focal events)
    df_bed = df_bed.sort_values(by=["gene", "svlen"], ascending=True)
    n_row_bef = df_bed.shape[0]
    df_bed = df_bed.drop_duplicates(subset=["gene"], keep="first")
    n_row_aft = df_bed.shape[0]
    print("-INFO: dropped %d/%d lines from genes overlapping multiple SV" % (n_row_bef-n_row_aft, n_row_bef))

    df_cna = df_bed.rename(columns={"gene": "Hugo_Symbol", "chrom": "Chromosome", "copy_number": "Copy_Number",
                                    "copy_number_more": "Copy_Number_More"})
    for col in ["tcn.em", "lcn.em", "svlen", "svstart", "svend", "overlap"]:
        df_cna[col] = df_cna[col].apply(convert_num_to_str).fillna("NA").astype(str)
    df_cna["TCN_EM:LCN_EM"] = df_cna[["tcn.em", "lcn.em"]].apply(":".join, axis=1)

    cols_gby = ["Hugo_Symbol", "Chromosome"]
    cols_agg = ["Copy_Number", "Copy_Number_More", "TCN_EM:LCN_EM", "svtype", "svlen", "svstart", "svend", "overlap"]
    dt_agg = {x: ";".join for x in cols_agg}

    # format columns
    for col in cols_agg:
        df_cna[col] = df_cna[col].apply(convert_num_to_str).fillna("NA").astype(str)

    if len(df_cna) > 0:
        df_cna = df_cna.groupby(cols_gby).agg(dt_agg).reset_index()
    else:
        df_cna = df_cna[cols_gby + cols_agg]

    # add tumor and normal sample ids
    basename = os.path.basename(args.input_bed)
    tsample = basename.split("_vs_")[0]

    if basename.endswith(".bed"):
        nsample = basename.split("_vs_")[1].split(".bed")[0]
    else:
        nsample = basename.split("_vs_")[1].split(".tsv")[0]

    df_cna.insert(0, "Tumor_Sample_Barcode", tsample)
    df_cna.insert(1, "Matched_Norm_Sample_Barcode", nsample)

    df_cna.to_csv(args.output, sep="\t", index=False)
    print("-filed saved at %s" % args.output)

# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert a table of CNV calls in bed format to a table of" \
                                     + " copy-number filtered calls.")
    parser.add_argument('--input_bed', type=str, help='Path to cnv table in bed format.')
    parser.add_argument('--threshold', type=int, help='Cnv events covering more than x Mb are discarded.',
                        default=10)
    parser.add_argument('--output', type=str, help='Path to output table.')
    args = parser.parse_args()

    for arg in vars(args):
        print("%s: %s" % (arg, getattr(args, arg)))
    print("\n")

    main(args)
