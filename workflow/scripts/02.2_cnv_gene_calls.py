# -*- coding: utf-8 -*-
"""
@created: Feb 01 2022
@modified: Nov 03 2022
@author: Yoann Pradat

    CentraleSupelec
    MICS laboratory
    9 rue Juliot Curie, Gif-Sur-Yvette, 91190 France

    Institut Gustave Roussy
    Prism Center
    114 rue Edouard Vaillant, Villejuif, 94800 France

Convert a VCF produced by cnv facets to a tsv table.
"""

import argparse
import gzip
import numpy as np
import os
import pandas as pd
import subprocess

# functions ============================================================================================================

def read_header(path):
    if path.endswith(".gz"):
        with gzip.open(path, "rt") as file:
            header = [x for x in file.readlines() if x.startswith("##")]
    else:
        with open(path, "r") as file:
            header = [x for x in file.readlines() if x.startswith("##")]
    return header


def read_table(path):
    header = read_header(path)
    df = pd.read_table(path, skiprows=len(header), na_values=["-","."])
    return df


def main(args):
    if args.output.endswith(".tsv.gz"):
        pattern = ".tsv.gz"
    else:
        pattern = ".tsv"

    bed_b = args.output.replace(pattern, "_b.bed")
    bed_i = args.output.replace(pattern, "_i.bed")

    # load table and genes
    df_tab = read_table(args.input_tab)

    # make bed from tab
    cols_bed = ["chrom", "start", "end", "tcn.em", "lcn.em", "svtype", "svlen", "copy_number", "copy_number_more"] + \
            ["Tumor_Sample_Barcode", "Matched_Norm_Sample_Barcode"]
    df_bed = df_tab[cols_bed].copy()
    for col in ["tcn.em", "lcn.em"]:
        df_bed[col] = df_bed[col].apply(lambda x: "%d" % x if not np.isnan(x) else x)

    df_bed.fillna(".").to_csv(bed_b, index=False, header=False, sep="\t")

    # run bedtools intersect
    cmd = "bedtools intersect -a %s -b %s -wao > %s" % (args.input_bed, bed_b, bed_i)
    print("-running the command:\n\t%s" % cmd)
    subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # read results from bedtools intersect
    df_bed_i = pd.read_table(bed_i, header=None, sep="\t")
    df_bed_i.columns = ["chrom_gene", "start_gene", "end_gene", "gene_id", "gene_name", "gene_biotype", "gene_source"] \
            + cols_bed + ["overlap"]

    # set "." to NA and set everything to NA for genes that could not be intersected
    df_bed_i = df_bed_i.replace(".", np.nan)
    mask_genes_mis = df_bed_i["chrom"].isnull()
    df_bed_i.loc[mask_genes_mis, list(df_bed.columns)] = np.nan

    for col in ["tcn.em", "lcn.em"]:
        df_bed_i[col] = df_bed_i[col].astype(float).apply(lambda x: "%d" % x if not np.isnan(x) else x)

    cols_old2new = {"start": "svstart", "end": "svend"}
    df_bed_i = df_bed_i.rename(columns=cols_old2new)

    # select columns keep
    cols_keep = ["Tumor_Sample_Barcode", "Matched_Norm_Sample_Barcode", "chrom_gene", "start_gene", "end_gene",
                 "gene_id", "gene_name", "gene_biotype", "gene_source", "tcn.em", "lcn.em", "overlap", "svtype",
                 "svstart", "svend", "svlen", "copy_number", "copy_number_more"]
    cols_old2new = {"chrom_gene": "chrom", "start_gene": "start", "end_gene": "end", "gene_name": "gene"}

    df_bed_o = df_bed_i[cols_keep]
    df_bed_o = df_bed_o.rename(columns=cols_old2new)

    # save and remove temporary files
    df_bed_o.to_csv(args.output, index=False, sep="\t")
    os.remove(bed_b)
    os.remove(bed_i)


# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Used a bed file of genes to infer gene-level CNAs.")
    parser.add_argument('--input_tab', type=str, help='Path to tsv file.')
    parser.add_argument('--input_bed', type=str, help='Path to bed file.')
    parser.add_argument('--output', type=str, help='Path to output table.')
    args = parser.parse_args()

    for arg in vars(args):
        print("%s: %s" % (arg, getattr(args, arg)))
    print("\n")

    main(args)
