# -*- coding: utf-8 -*-
"""
@created: Nov 23 2022
@modified: Nov 23 2022
@author: Yoann Pradat

    CentraleSupelec
    MICS laboratory
    9 rue Juliot Curie, Gif-Sur-Yvette, 91190 France

    Institut Gustave Roussy
    Prism Center
    114 rue Edouard Vaillant, Villejuif, 94800 France

Script to select only tumor/normal pairs that need to be run.
"""

import argparse
import datetime
import os
import pandas as pd
import subprocess

# functions ============================================================================================================

def main(args):
    # load table
    df_tnp = pd.read_table(args.pairs_table)

    # check if expected output files for each pair already exist and were updated after the user-specified date, if any.
    pairs = df_tnp["DNA_P"].tolist()
    folders = ["results/annotation/somatic_cna_civic", "results/annotation/somatic_cna_oncokb"]
    pairs_run = []

    if args.update_date_min is not None:
        update_datetime_min = datetime.datetime.strptime(args.update_date_min, "%d/%m/%Y")
    else:
        update_datetime_min = None

    for pair in pairs:
        print("-INFO: checking whether pair %s needs to be run or not..." % pair)

        folders_lines_out = []
        for folder in folders:
            gs_file = os.path.join(args.bucket_gs_uri, folder, "%s.tsv" % pair)
            cmd = "gsutil stat %s" % gs_file
            cmd_out = subprocess.run(cmd, shell=True, capture_output=True)
            lines_out = cmd_out.stdout.decode("utf-8").split("\n")
            folders_lines_out.append(lines_out)

        if any(lines_out[0].startswith("No URLs matched") for lines_out in folders_lines_out):
            # at least one expected output is missing
            print("--one of the ouput file does not exist, RUN")
            pairs_run.append(pair)
        else:
            # only if a minimal update date is specified, check the update times of expected output files
            if update_datetime_min is not None:
                for lines_out in folders_lines_out:
                    line_update = [x for x in lines_out if "Update time" in x]
                    if not len(line_update)==0:
                        update_date = line_update[0].split("Update time:")[1].strip()
                        update_date = " ".join(update_date.split()[1:4])
                        update_datetime = datetime.datetime.strptime(update_date, '%d %b %Y')
                        if update_datetime < update_datetime_min:
                            print("--one of the ouput file has update date %s < %s, RUN" % \
                                  (update_date, args.update_date_min))
                            pairs_run.append(pair)
                            break

    print("-INFO: %d/%d pairs need to be run" % (len(pairs_run), len(pairs)))
    df_tnp = df_tnp.loc[df_tnp["DNA_P"].isin(pairs_run)].copy()
    df_tnp.to_csv(args.pairs_table, sep="\t", index=False)
    print("-updated file %s accordingly" % args.pairs_table)

# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Subset the table of pairs.")
    parser.add_argument('--pairs_table', type=str, help='Path to input table.', default="config/tumor_normal_pairs.tsv")
    parser.add_argument('--bucket_gs_uri', type=str, help='Google cloud storage URI to bucket.',
                        default="gs://facets_tcga_results")
    parser.add_argument('--update_date_min', type=str,
                        help='Date last update of output files should be more recent that this date. DD/MM/YYYY format.',
                        default=None)
    args = parser.parse_args()

    main(args)
