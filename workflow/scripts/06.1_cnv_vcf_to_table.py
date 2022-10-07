# -*- coding: utf-8 -*-
"""
@created: Feb 01 2022
@modified: Oct 03 2022
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


def save_table_with_header(df, header, output):
    output_header = os.path.join(os.path.dirname(output), "header.tsv")

    print("-writing header in file %s" % output_header)
    with open(output_header, "w") as file_header:
        for line in header:
            file_header.write(line)

    # write contents
    if output.endswith(".gz"):
        output_uncompressed = output[:-3] + ".tmp"
        output_concatenate = output[:-3]
    else:
        output_uncompressed = output + ".tmp"
        output_concatenate = output

    print("-writing contents in file %s" % output_uncompressed)
    df.to_csv(output_uncompressed, index=False, sep="\t")

    # # concat both files
    cmd = "cat %s %s >> %s" % (output_header, output_uncompressed, output_concatenate)
    print("-running the command:\n\t%s" % cmd)
    subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    os.remove(output_header)
    os.remove(output_uncompressed)

    # compress if required
    if output_concatenate != output:
        cmd = "gzip %s" % output_concatenate
        print("-running the command:\n\t%s" % cmd)
        subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def make_prefix_header(df, col):
    vals = df[col].dropna().values
    headers = list(set([val .split("=")[0] for val in vals]))
    if len(headers)>1:
        print("-warning! multiple headers were found for col %s" % col)
        print("\t" + "\n\t".join(headers))
    df[col] = df[col].apply(lambda x: x.split("=")[1] if type(x)==str else x)
    df = df.rename(columns={col: headers[0]})
    return df


def extract_from_header(header, field, prefix="##"):
    regex = "%s%s=" % (prefix, field)
    line = [x.strip() for x in header if x.startswith(regex)]

    if len(line)==0:
        print("WARNING: the pattern %s was not found!" % regex)
        value = None
    elif len(line)>1:
        print("WARNING: the pattern %s was found more than once!" % regex)
        value = None
    else:
        value = float(line[0].replace(regex, ""))

    return value


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
    # check args
    if args.gender.lower() not in ["male", "female"]:
        raise ValueError("please specify a value for --gender that is either 'male' or 'female' (case-insensitive).")

    # load table and genes
    header_vcf = read_header(args.input)
    df_vcf = read_table(args.input)

    # reformat INFO
    df_info = df_vcf["INFO"].str.split(";").apply(pd.Series)
    for col in df_info.columns:
        df_info = make_prefix_header(df_info, col)

    # concat
    df_tab = pd.concat((df_vcf, df_info), axis=1)

    # rename columns
    df_tab.columns = [x.lower().replace("_", ".") for x in df_tab.columns]
    old2new = {"#chrom": "chrom", "id": "seg", "pos": "start", "maf.r": "mafR", "maf.r.clust": "mafR.clust"}
    df_tab = df_tab.rename(columns=old2new)

    # columns order and keep
    cols = ["chrom", "seg", "num.mark", "nhet", "cnlr.median", "mafR", "segclust", "cnlr.median.clust", "mafR.clust",
            "start", "end", "cf.em", "tcn.em", "lcn.em", "svtype", "svlen"]
    df_tab = df_tab[cols]

    # replace . by NA
    df_tab = df_tab.replace(".", np.nan)

    # extract metrics from header
    ploidy = extract_from_header(header=header_vcf, field="ploidy")

    # compute average ploidy for each segment taking into account gender
    df_tab["ploidy"] = ploidy
    if args.gender.lower()=="male":
        chroms_male = ["X", "Y", "23", "24", 23, 24]
        df_tab.loc[df_tab["chrom"].isin(chroms_male), "ploidy"] /= 2
    ploidy = df_tab["ploidy"]
    del df_tab["ploidy"]

    # set numeric types for numeric cols
    cols_num = ["tcn.em", "lcn.em", "svlen"]
    for col_num in cols_num:
        df_tab[col_num] = df_tab[col_num].astype(float)

    # classify gains into the 3 following categories
    #   - HL_amp -> tcn.em > 3 x sample avg ploidy 
    #   - ML_amp -> tcn.em > 2 x sample avg ploidy 
    #   - LL_amp -> tcn.em > 1.4 x sample avg ploidy 
    hl_thresh = 3
    ml_thresh = 2
    ll_thresh = 1.4
    mask_hl = df_tab["tcn.em"]/ploidy > hl_thresh
    mask_ml = (df_tab["tcn.em"]/ploidy > ml_thresh) & (~mask_hl)
    mask_ll = (df_tab["tcn.em"]/ploidy > ll_thresh) & (~mask_hl) & (~mask_ml)
    nsv_hl = df_tab.loc[mask_hl]["svlen"].nunique()
    chr_hl = df_tab.loc[mask_hl]["chrom"].unique().tolist()
    nsv_ml = df_tab.loc[mask_ml]["svlen"].nunique()
    chr_ml = df_tab.loc[mask_ml]["chrom"].unique().tolist()
    nsv_ll = df_tab.loc[mask_ll]["svlen"].nunique()
    chr_ll = df_tab.loc[mask_ll]["chrom"].unique().tolist()

    df_hl_gain = df_tab.loc[mask_hl].copy()
    df_hl_gain["copy_number_more"] = "HL_gain"
    df_hl_gain["copy_number"] = 2
    df_ml_gain = df_tab.loc[mask_ml].copy()
    df_ml_gain["copy_number_more"] = "ML_gain"
    # df_ml_gain["copy_number"] = 2
    df_ll_gain = df_tab.loc[mask_ll].copy()
    df_ll_gain["copy_number_more"] = "LL_gain"

    print("-INFO: identified %d HL amp SV (chr %s)" % (nsv_hl, ";".join(chr_hl)))
    print("-INFO: identified %d ML amp SV (chr %s)" % (nsv_ml, ";".join(chr_ml)))
    print("-INFO: identified %d LL amp SV (chr %s)" % (nsv_ll, ";".join(chr_ll)))

    df_gain = pd.concat((df_hl_gain, df_ml_gain, df_ll_gain))

    # classify losses into the 3 following categories
    #   - hom_del -> lcn.em = 0, tcn.em = 0
    #   - LOH -> lcn.em = 0, tcn.em  < 0.6 x sample avg ploidy
    #   - cnLOH -> lcn.em = 0, 0.6 x sample avg ploidy < tcn.em  < 1.4 x sample avg ploidy
    thresh_a = 0.6
    thresh_b = 1.4

    mask_homdel = (df_tab["lcn.em"] == 0) & (df_tab["tcn.em"] == 0)
    mask_loh =  (df_tab["lcn.em"] == 0) & (df_tab["tcn.em"]/ploidy < thresh_a) & (~mask_homdel)
    mask_cnloh = (df_tab["lcn.em"] == 0) & (df_tab["tcn.em"]/ploidy > thresh_a) & (df_tab["tcn.em"]/ploidy < thresh_b)
    nsv_homdel = df_tab.loc[mask_homdel]["svlen"].nunique()
    chr_homdel = df_tab.loc[mask_homdel]["chrom"].unique().tolist()
    nsv_loh = df_tab.loc[mask_loh]["svlen"].nunique()
    chr_loh = df_tab.loc[mask_loh]["chrom"].unique().tolist()
    nsv_cnloh = df_tab.loc[mask_cnloh]["svlen"].nunique()
    chr_cnloh = df_tab.loc[mask_cnloh]["chrom"].unique().tolist()

    df_homdel_loss = df_tab.loc[mask_homdel].copy()
    df_homdel_loss["copy_number_more"] = "hom_del"
    df_homdel_loss["copy_number"] = -2
    df_loh_loss = df_tab.loc[mask_loh].copy()
    df_loh_loss["copy_number_more"] = "LOH"
    df_cnloh_loss = df_tab.loc[mask_cnloh].copy()
    df_cnloh_loss["copy_number_more"] = "cnLOH"

    print("-INFO: identified %d HOM DEL SV (chr %s)" % (nsv_homdel, ";".join(chr_homdel)))
    print("-INFO: identified %d LOH SV (chr %s)" % (nsv_loh, ";".join(chr_loh)))
    print("-INFO: identified %d cnLOH SV (chr %s)" % (nsv_cnloh, ";".join(chr_cnloh)))

    df_loss = pd.concat((df_homdel_loss, df_loh_loss, df_cnloh_loss))

    # concat gain and loss and reorder
    df_cna = pd.concat((df_gain, df_loss)).sort_index()
    df_tab = df_tab.merge(df_cna, how="left")

    # revert num to cols to avoid trailing zeroes
    for col_num in cols_num:
        df_tab[col_num] = df_tab[col_num].apply(convert_num_to_str)

    # save
    if not args.keep_header:
        df_tab.to_csv(args.output, index=False, sep="\t")
    else:
        save_table_with_header(df_tab, header, args.output)


# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract and classify copy-number segments from VCF.")
    parser.add_argument('--input', type=str, help='Path to vcf file.',
                        default="results/calling/somatic_cnv_facets/MR358-T1-ADN_vs_MR358-N.vcf.gz")
    parser.add_argument('--gender', type=str, help='Gender of the sample. Either "Male" or "Female".',
                        default="Male")
    parser.add_argument("--keep_header", action="store_true", default=False,
                        help="If used, the header of the maf tables is preserved.")
    parser.add_argument('--output', type=str, help='Path to output table.',
                        default="results/calling/somatic_cnv_table/MR358-T1-ADN_vs_MR358-N.tsv.gz")
    args = parser.parse_args()

    for arg in vars(args):
        print("%s: %s" % (arg, getattr(args, arg)))
    print("\n")

    main(args)
