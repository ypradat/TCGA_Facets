from itertools import product
import pandas as pd
import re
from snakemake.utils import validate
from snakemake.utils import min_version

min_version("5.4.0")

B_FOLDER = "workflow/benchmarks"
L_FOLDER = "workflow/logs"
R_FOLDER = "results"

###### Config file and sample sheets #####
configfile: "config/config.yaml"

table = pd.read_table(config["samples"], dtype=str).set_index(["Sample_Id"], drop=False)
samples = table["Sample_Id"].tolist()
nsamples = table.loc[(table["Sample_Type"]=="DNA_N")]["Sample_Id"].tolist()
nsamples_na = nsamples + ["NA"]
tsamples = table.loc[(table["Sample_Type"]=="DNA_T")]["Sample_Id"].tolist()

##### Helper functions #####

def filter_combinator(combinator, comblist, white_list=True):
    def filtered_combinator(*args, **kwargs):
        for wc_comb in combinator(*args, **kwargs):
        # Use frozenset instead of tuple
        # in order to accomodate
        # unpredictable wildcard order
            if white_list:
                if frozenset(wc_comb) in comblist:
                    yield wc_comb
            else:
                if frozenset(wc_comb) not in comblist:
                    yield wc_comb
    return filtered_combinator


def get_allowed_pairs_tumor_normal():
    allowed = []
    df_pairs = pd.read_table(config["tumor_normal_pairs"]).fillna("NA")
    for (tsample, nsample) in zip(df_pairs["DNA_T"], df_pairs["DNA_N"]):
        allowed.append(frozenset({("tsample", tsample), ("nsample", nsample)}))
    return filter_combinator(product, allowed, white_list=True)


def get_column_table_sample(wildcards, col):
    """Get the value of the column col for the sample"""
    try:
        value = table.loc[wildcards.sample, col]
    except AttributeError:
        try:
            value = table.loc[wildcards.tsample, col]
        except AttributeError:
            try:
                value = table.loc[wildcards.nsample, col]
            except AttributeError:
                if wildcards.sample_pair=="all_samples":
                    value = ""
                else:
                    tsample = wildcards.sample_pair.split("_vs_")[0]
                    value = table.loc[tsample, col]
    return value


def get_input_concatenate(w, typ, db):
    input_folder = "%s/annotation/somatic_%s_%s_filter" % (R_FOLDER, typ, db)

    if config["params"][db]["run_per_sample"][typ]:
        sample_pairs = expand("{tsample}_vs_{nsample}", get_allowed_pairs_tumor_normal(),
            tsample=tsamples, nsample=nsamples_na)
    else:
        sample_pairs = ["all_samples"]

    if typ=="maf":
        return ["%s/%s.maf" % (input_folder, sample_pair) for sample_pair in sample_pairs]
    elif typ=="cna":
        return ["%s/%s.tsv" % (input_folder, sample_pair) for sample_pair in sample_pairs]
