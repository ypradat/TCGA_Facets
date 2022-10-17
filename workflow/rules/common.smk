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


def get_load_snp_pileup(wildcards):
    df_tnp = pd.read_table(config["tumor_normal_pairs"])
    dna_p = "%s_vs_%s" % (wildcards.tsample, wildcards.nsample)
    size_dna_p = df_tnp.loc[df_tnp["DNA_P"]==dna_p, "File_Size_P"].iloc[0]

    # assume a total available load of 115
    # assume a pair file size below 50 gb will consume less than 12 gb RAM, allow to run 5 pairs in parallel
    # assume a pair file size below 100gb will consume less than 20 gb RAM, allow to run 3 pairs in parallel
    # assume a pair file size above 100gb will consume less than 30 gb RAM, allow to run 2 pairs in parallel
    if size_dna_p >= 100:
        load = 50
    elif size_dna_p >= 50:
        load = 30
    else:
        load = 20

    return load


def get_threads_snp_pileup(wildcards):
    n_tnp = pd.read_table(config["tumor_normal_pairs"]).shape[0]
    load = get_load_snp_pileup(wildcards)

    if load==50:
        threads = 8
    elif load==30:
        if n_tnp >= 4:
            threads = 4
        elif n_tnp == 3:
            threads = 5
        elif n_tnp == 2:
            threads = 8
        else:
            threads = 16
    else:
        if n_tnp >= 4:
            threads = 3
        elif n_tnp == 3:
            threads = 4
        elif n_tnp == 2:
            threads = 8
        else:
            threads = 16

    return threads
