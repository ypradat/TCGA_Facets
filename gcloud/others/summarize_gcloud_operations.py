# -*- coding: utf-8 -*-
"""
@created: Oct 15 2022
@modified: Oct 15 2022
@author: Yoann Pradat

    CentraleSupelec
    MICS laboratory
    9 rue Juliot Curie, Gif-Sur-Yvette, 91190 France

    Institut Gustave Roussy
    Prism Center
    114 rue Edouard Vaillant, Villejuif, 94800 France

Build a table of gcloud operations with one line per instance.
"""

import argparse
import os
import pandas as pd
import subprocess
from functools import reduce

# functions ============================================================================================================

def get_operations_table(operation_type, target_link="instances/facets-tcga-[\d]+"):
    filter="operationType~%s AND targetLink~%s" % (operation_type, target_link)
    format="(NAME, TYPE, TARGET, targetId, STATUS, TIMESTAMP)"
    cmd = 'gcloud compute operations list --filter="%s" --format="%s"' % (filter, format)
    cmd_output = subprocess.run(cmd, shell=True, capture_output=True)
    cmd_stdout = cmd_output.stdout.decode("utf-8")
    lines = [x.split() for x in cmd_stdout.split("\n") if x!=""]
    if len(lines)==1:
        return pd.DataFrame(columns=lines[0])
    elif len(lines)==1:
        return pd.DataFrame()
    else:
        df = pd.DataFrame(lines[1:], columns=lines[0])
        df["TARGET_NAME"] = df["TARGET"].apply(lambda x: os.path.basename(x))
        df["TIMESTAMP_DT"] = pd.to_datetime(df["TIMESTAMP"])
        return df


def pivot_duplicate(df, cols_index, col_value):
    df_value  = df[cols_index + [col_value]]
    i_cur = 0
    r_pre = df_value.iloc[0]
    labels = []
    for _, r_cur in df_value.iterrows():
        if r_cur[cols_index].equals(r_pre[cols_index]):
            i_cur += 1
        else:
            i_cur = 1
        r_pre = r_cur

        label = "%s_%d" % (col_value, i_cur)
        labels.append(label)

    df_value["LABEL"] = labels
    df_value = df_value.pivot(values=col_value, index=cols_index, columns="LABEL")
    df_value.columns.name = None
    return df_value


def compute_runtime(row):
    time_insert = row["TIMESTAMP_INSERT"]
    time_delete = row["TIMESTAMP_DELETE"]
    cols_preempted = sorted([x for x in row.index if x.startswith("TIMESTAMP_PREEMPTED")])
    cols_restart = sorted([x for x in row.index if x.startswith("TIMESTAMP_RESTART")])
    times_preempted = [row[col_preempted] for col_preempted in cols_preempted]
    times_restart = [row[col_restart] for col_restart in cols_restart]

    runtime = (pd.to_datetime(time_delete)-pd.to_datetime(time_insert)).seconds/3600

    if (~row[cols_preempted].isnull()).sum() != 0:
        # remove time intervals during which the machine was stopped
        for time_preempted, time_restart in zip(times_preempted, times_restart):
            runtime_stop = (pd.to_datetime(time_restart)-pd.to_datetime(time_preempted)).seconds/3600
            runtime -= runtime_stop

    return runtime


def main(args):
    # load table
    df_sam = pd.read_table(args.samples_table)

    # list insert (i.e instance creation) operations
    df_insert = get_operations_table(operation_type="insert")

    # list delete (i.e instance deletion) operations
    df_delete = get_operations_table(operation_type="delete")

    # list preemption (i.e instance preemption) operations
    df_preempted = get_operations_table(operation_type="preempted")

    # list start (i.e instance restart after preemption) operations
    df_start = get_operations_table(operation_type="start")

    # select only latest instance creation
    df_insert = df_insert.sort_values(by=["TARGET_NAME", "TIMESTAMP_DT"], ascending=True)
    df_insert = df_insert.drop_duplicates(subset=["TARGET_NAME"], keep="last")

    # rename columns and merge iteratively
    cols_target = ["TARGET", "TARGET_NAME", "TARGET_ID"]
    cols_time = ["TIMESTAMP"]
    cols_keep = cols_target + cols_time

    # only one insert
    suffix = "INSERT"
    old2new = {col: "%s_%s" % (col, suffix) for col in cols_time}
    df_insert = df_insert[cols_keep].rename(columns=old2new)

    # multiple preemptions may have occured.
    suffix = "PREEMPTED"
    old2new = {col: "%s_%s" % (col, suffix) for col in cols_time}
    cols_time_suffix = ["%s_%s" % (col, suffix) for col in cols_time]
    df_preempted = df_preempted[cols_keep].rename(columns=old2new)
    df_preempted = df_preempted.sort_values(by=cols_target+cols_time_suffix[-1:], ascending=True)
    dfs_preempted = [pivot_duplicate(df_preempted, cols_target, x) for x in cols_time_suffix]
    df_preempted = reduce(lambda df1,df2: pd.merge(df1,df2,on=cols_target), dfs_preempted)

    # multiple restarts may have occured.
    suffix = "RESTART"
    old2new = {col: "%s_%s" % (col, suffix) for col in cols_time}
    cols_time_suffix = ["%s_%s" % (col, suffix) for col in cols_time]
    df_start = df_start[cols_keep].rename(columns=old2new)
    df_start = df_start.sort_values(by=cols_target+cols_time_suffix[-1:], ascending=True)
    dfs_start = [pivot_duplicate(df_start, cols_target, x) for x in cols_time_suffix]
    df_start = reduce(lambda df1,df2: pd.merge(df1,df2,on=cols_target), dfs_start)

    # only one delete
    suffix = "DELETE"
    old2new = {col: "%s_%s" % (col, suffix) for col in cols_time}
    df_delete = df_delete[cols_keep].rename(columns=old2new)

    # merge left
    df_table = df_insert.copy()
    df_table = df_table.merge(df_preempted, how="left", on=cols_target)
    df_table = df_table.merge(df_start, how="left", on=cols_target)
    df_table = df_table.merge(df_delete, how="left", on=cols_target)

    # compute runtime
    df_table["RUNTIME_HOUR"] = df_table.apply(compute_runtime, axis=1)

    # compute disk space taking into account 50gb margin
    df_sam = df_sam.rename(columns={"Batch": "BATCH"})
    df_sam["TARGET_NAME"] = "facets-tcga-" + df_sam["BATCH"].astype(str)
    df_disk_size = df_sam.groupby(["TARGET_NAME", "BATCH"])["Total_Size_Gb"].sum().to_frame("DISK_SIZE").reset_index()
    df_disk_size["DISK_SIZE"] = df_disk_size["DISK_SIZE"] + 50
    df_disk_size["DISK_SIZE"] = df_disk_size["DISK_SIZE"].apply(lambda x: int(x)).astype(int)
    df_table = df_table.merge(df_disk_size, how="left", on="TARGET_NAME")

    # estimate cost
    base_hourly_cost = 391.35/(24*30.5)
    disk_hourly_cost_per_gb = 0.1/(24*30.5)
    df_table["COST_STANDARD"] = df_table["RUNTIME_HOUR"] * (base_hourly_cost + disk_hourly_cost_per_gb
                                                            * df_table["RUNTIME_HOUR"])
    df_table["COST_PREEMPTIBLE_60PCT_DISCOUNT"] = (1-0.60) * df_table["COST_STANDARD"]
    df_table["COST_PREEMPTIBLE_91PCT_DISCOUNT"] = (1-0.91) * df_table["COST_STANDARD"]

    # save table
    df_table["TARGET_ID"] = df_table["TARGET_ID"].astype(str)
    df_table["BATCH"] = df_table["BATCH"].astype(int)
    df_table = df_table.sort_values(by="BATCH")
    df_table.to_excel(args.operations_table, index=False, float_format="%.3f")
    print("-table saved at %s" % args.operations_table)

    # get total, ignore first 5 batches that were not optimized yet
    n_batches = df_sam["BATCH"].nunique()
    avg_cost_per_batch_std = df_table.loc[df_table["BATCH"]>5]["COST_STANDARD"].mean()
    avg_cost_per_batch_60pct = df_table.loc[df_table["BATCH"]>5]["COST_PREEMPTIBLE_60PCT_DISCOUNT"].mean()
    avg_cost_per_batch_91pct = df_table.loc[df_table["BATCH"]>5]["COST_PREEMPTIBLE_91PCT_DISCOUNT"].mean()

    total_cost_std = n_batches * avg_cost_per_batch_std
    total_cost_60pct = n_batches * avg_cost_per_batch_60pct
    total_cost_91pct = n_batches * avg_cost_per_batch_91pct
    print("POSSIBLE COSTS ARE:")
    print("\t-STANDARD: $%.2f (avg cost/batch $%.3f)" % (total_cost_std, avg_cost_per_batch_std))
    print("\t-PREEMPTIBLE 60%% DISCOUNT: $%.2f (avg cost/batch $%.3f)" % (total_cost_60pct, avg_cost_per_batch_60pct))
    print("\t-PREEMPTIBLE 91%% DISCOUNT: $%.2f (avg cost/batch $%.3f)" % (total_cost_91pct, avg_cost_per_batch_91pct))


# run ==================================================================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build a table of gcloud operations with one line per instance.")
    parser.add_argument('--samples_table', type=str, help='Path to samples table.',
                        default="config/samples.all.tsv")
    parser.add_argument('--operations_table', type=str, help='Path to output table of operations.',
                        default="workflow/logs/gcloud_operations.xlsx")
    args = parser.parse_args()

    main(args)
