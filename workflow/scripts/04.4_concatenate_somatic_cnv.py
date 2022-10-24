# -*- coding: utf-8 -*-
"""
@created: May 03 2022
@modified: May 03 2022
@author: Yoann Pradat

    CentraleSupelec
    MICS laboratory
    9 rue Juliot Curie, Gif-Sur-Yvette, 91190 France

    Institut Gustave Roussy
    Prism Center
    114 rue Edouard Vaillant, Villejuif, 94800 France

Table of CNA counts for a particular gene.
"""

import argparse
import os
import numpy as np
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
    # load per sample calls
    files = args.cnv
    dfs_cna = []
    for i, file in enumerate(files):
        df_cna = pd.read_table(file)

        if "Tumor_Sample_Barcode" not in df_cna and "Matched_Norm_Sample_Barcode" not in df_cna:
            basename = os.path.basename(file)
            tsample = basename.split("_vs_")[0]
            if ".bed" in basename:
                nsample = basename.split("_vs_")[1].split(".bed")[0]
            elif ".tsv" in basename:
                nsample = basename.split("_vs_")[1].split(".tsv")[0]
            df_cna.insert(0, "Tumor_Sample_Barcode", tsample)
            df_cna.insert(1, "Matched_Norm_Sample_Barcode", nsample)

        dfs_cna.append(df_cna)
        if (i+1)%(len(files)//100)==0:
            print("-processed %d/%d files" % (i+1, len(files)), flush=True)

    df_cna = pd.concat(dfs_cna)

    # add FILTER
    if "svlen" in df_cna and "FILTER" not in df_cna:
        mask_pass = df_cna["svlen"] < args.threshold * 1e6
        df_cna.loc[mask_pass, "FILTER"] = "PASS"
        df_cna.loc[~mask_pass, "FILTER"] = "SV > %d Mb" % args.threshold

    # format numeric values
    cols_num = ["chrom", "start", "end", "tcn.em", "lcn.em", "overlap", "svlen"]
    for col_num in cols_num:
        if col_num in df_cna:
            df_cna[col_num] = df_cna[col_num].fillna("NA").apply(convert_num_to_str)

    # save
    df_cna.to_csv(args.output, sep="\t", index=False)
    print("-file saved at %s" % args.output)

# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Concatenate tables of CNAs and add FILTER tag.")
    parser.add_argument("--cnv", type=str, nargs="+", help="Path to input tables of CNVs.")
    parser.add_argument('--threshold', type=int, help='Cnv events covering more than x Mb are discarded.',
                        default=10)
    parser.add_argument('--output', type=str,  help='Paths to output table.')
    args = parser.parse_args()

    for arg in vars(args):
        print("%s: %s" % (arg, getattr(args, arg)))
    print("\n", end="")

    main(args)
