# -*- coding: utf-8 -*-
"""
@created: Oct 04 2022
@modified: Oct 06 2022
@author: Yoann Pradat

    CentraleSupelec
    MICS laboratory
    9 rue Juliot Curie, Gif-Sur-Yvette, 91190 France

    Institut Gustave Roussy
    Prism Center
    114 rue Edouard Vaillant, Villejuif, 94800 France

Simplify and concatenates tables of chromosome arm events.
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


def load_cnv_data(input_chr_arm, input_cnv_sum, col_tsb, col_nsb):
    dfs_chr_arm = []
    dfs_cnv_sum = []

    for file_chr_arm, file_cnv_sum in zip(input_chr_arm, input_cnv_sum):
        df_chr_arm_pair = pd.read_table(file_chr_arm)
        df_cnv_sum_pair = pd.read_table(file_cnv_sum)

        if col_tsb not in df_chr_arm_pair and col_nsb not in df_chr_arm_pair:
            basename = os.path.basename(file_chr_arm)
            tsample = basename.split("_vs_")[0]
            if ".bed" in basename:
                nsample = basename.split("_vs_")[1].split(".bed")[0]
            elif ".tsv" in basename:
                nsample = basename.split("_vs_")[1].split(".tsv")[0]
            df_chr_arm_pair.insert(0, col_tsb, tsample)
            df_chr_arm_pair.insert(1, col_nsb, nsample)

        dfs_chr_arm.append(df_chr_arm_pair)
        dfs_cnv_sum.append(df_cnv_sum_pair)

    df_chr_arm = pd.concat(dfs_chr_arm)
    df_cnv_sum = pd.concat(dfs_cnv_sum)

    return df_chr_arm, df_cnv_sum



def main(args):
    col_tsb = "Tumor_Sample_Barcode"
    col_nsb = "Matched_Norm_Sample_Barcode"

    # load per sample calls
    df_chr_arm, df_cnv_sum = load_cnv_data(args.input_chr_arm, args.input_cnv_sum, col_tsb, col_nsb)
    cols_chr_arm = df_chr_arm.columns.tolist()

    # add WGD status and ploidy
    df_chr_arm = df_chr_arm.merge(df_cnv_sum[[col_tsb, col_nsb, "Ploidy", "WGD"]], how="left", on=[col_tsb, col_nsb])

    # add gender
    df_cln = pd.read_table(args.input_cln)
    df_cln = df_cln.rename(columns={"DNA_T": col_tsb, "DNA_N": col_nsb})
    df_chr_arm = df_chr_arm.merge(df_cln[[col_tsb, col_nsb, "Gender"]], how="left", on=[col_tsb, col_nsb])

    # add X_Male status
    mask_male = df_chr_arm["Gender"].str.lower()=="male"
    mask_xchr = df_chr_arm["arm"].isin(["23p", "23q"])
    df_chr_arm["X_Male"] = 0
    df_chr_arm.loc[mask_male & mask_xchr, "X_Male"] = 1

    # build key to match with rules
    df_rules = pd.read_excel(args.rules)
    df_rules = df_rules.fillna("NA")

    # first check if abolute or relative CN should be used
    df_chr_arm = df_chr_arm.merge(df_rules[["WGD", "X_Male", "Ratio_To_Ploidy"]].drop_duplicates(), how="left")
    mask_ratio = df_chr_arm["Ratio_To_Ploidy"]==1
    df_chr_arm["TCN_Key"] = df_chr_arm["tcn"]
    df_chr_arm.loc[mask_ratio, "TCN_Key"] = df_chr_arm.loc[mask_ratio, "tcn"]/df_chr_arm.loc[mask_ratio, "Ploidy"]
    df_chr_arm["LCN_Key"] = df_chr_arm["lcn"]
    df_chr_arm.loc[mask_ratio, "LCN_Key"] = df_chr_arm.loc[mask_ratio, "lcn"]/df_chr_arm.loc[mask_ratio, "Ploidy"]

    # execute rules line by line
    for _, rule in df_rules.iterrows():
        mask_rule = df_chr_arm["WGD"]==rule["WGD"]
        mask_rule &= df_chr_arm["X_Male"]==rule["X_Male"]
        for cn in ["TCN", "LCN"]:
            if rule[cn] != "NA":
                if type(rule[cn])==str and "," in rule[cn]:
                    rule_cn_low, rule_cn_high = rule[cn].split(",")
                    mask_rule &= eval("df_chr_arm['%s_Key'] %s" % (cn, rule_cn_low))
                    mask_rule &= eval("df_chr_arm['%s_Key'] %s" % (cn, rule_cn_high))
                else:
                    try:
                        value_cn = rule[cn]
                        mask_rule &= eval("df_chr_arm['%s_Key'] == %s" % (cn, value_cn))
                    except:
                        rule_cn = rule[cn]
                        mask_rule &= eval("df_chr_arm['%s_Key'] %s" % (cn, rule_cn))

        df_chr_arm.loc[mask_rule, "State"] = rule["State"]
        df_chr_arm.loc[mask_rule, "State_More"] = rule["State_More"]

    # select columns
    cols_keep = cols_chr_arm + ["Ploidy", "WGD", "State", "State_More"]
    df_chr_arm = df_chr_arm[cols_keep]

    # fill empty State and State_More
    mask_null = df_chr_arm["State"].isnull()
    df_chr_arm.loc[mask_null, "State"] = 0
    df_chr_arm.loc[mask_null, "State_More"] = "NEUTR"
    df_chr_arm["State"] = df_chr_arm["State"].astype(int)

    # save
    df_chr_arm.to_csv(args.output, sep="\t", index=False)

# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Concatenate tables of CNAs per chromosome arm.")
    parser.add_argument("--input_chr_arm", type=str, nargs="+", help="Path to input tables of CNV per chromosome arm.")
    parser.add_argument("--input_cnv_sum", type=str, nargs="+", help="Path to input tables of CNA summary statistics.")
    parser.add_argument("--input_cln", type=str, help="Path to input clinical table providing gender.")
    parser.add_argument("--rules", type=str, help="Path to table of rules for calling chromosome arm events.")
    parser.add_argument('--output', type=str,  help='Paths to output table.')
    args = parser.parse_args()

    for arg in vars(args):
        print("%s: %s" % (arg, getattr(args, arg)))
    print("\n", end="")

    main(args)
