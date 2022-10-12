import argparse
import numpy as np
import pandas as pd

def main(args):
    # read samples data
    filepath = "../../data/tcga/clinical/curated_other/bio_tcga_all_curated.tsv"
    df_bio = pd.read_table(filepath, low_memory=False)

    # keep only DNA
    df_bio = df_bio.loc[df_bio["Sample_Type"].isin(["DNA_N", "DNA_T"])]
    df_bio["Sample_Id"] = df_bio["Aliquot_Id"]

    # keep only sequencing
    mask_seq = df_bio["Data_Category_Gdc"] == "Sequencing Reads"
    df_bio = df_bio.loc[mask_seq].copy()

    # keep only WXS
    mask_wxs = df_bio["Experimental_Strategy_Gdc"] == "WXS"
    df_bio = df_bio.loc[mask_wxs].copy()

    # cols selection
    cols_ids = ["Sample_Id", "File_Id_Gdc", "File_Name_Gdc"]
    cols_bio = ["Biopsy_Type", "Sample_Type"]

    # add Project_TCGA_More, MSKCC_Oncotree and Civic_Disease
    filepath = "../../data/tcga/clinical/curated_other/cln_tcga_all_curated.tsv"

    df_cln = pd.read_table(filepath)
    cols_cln = ["Subject_Id", "Project_TCGA_More", "MSKCC_Oncotree", "Civic_Disease", "Gender"]
    cols_cln = [x for x in cols_cln if x in df_cln.columns]
    df_bio = df_bio.merge(df_cln[cols_cln], how="left", on="Subject_Id")

    # select columns
    cols = cols_ids+cols_bio+cols_cln
    df_sam = df_bio[cols].copy()

    # intersect with table from ISB-CGC
    df_bio_isb = pd.read_table("../../data/tcga/clinical/raw_bio_files/tcga_wxs_bams_isb_cgc.csv", sep=",")
    old2new = {"aliquot_barcode": "Sample_Id",
               "file_name_key": "File_Name_Key",
               "file_size": "File_Size",
               "index_file_name_key": "Index_File_Name_Key",
               "index_file_size": "Index_File_Size"}
    df_bio_isb = df_bio_isb.rename(columns=old2new)
    df_bio_isb = df_bio_isb[list(old2new.values())].drop_duplicates()
    df_sam = df_sam.merge(df_bio_isb, how="left", on="Sample_Id")

    # drop samples with missing values
    df_sam = df_sam.dropna(how="any")

    # extract File_Name and Index_File_Name
    df_sam["File_Name"] = df_sam["File_Name_Key"].apply(lambda x: x.split("/")[-1])
    df_sam["Index_File_Name"] = df_sam["Index_File_Name_Key"].apply(lambda x: x.split("/")[-1])
    df_sam["File_Size"] = df_sam["File_Size"].astype(int)
    df_sam["Index_File_Size"] = df_sam["Index_File_Size"].astype(int)

    # prepare table of tumor/normal pairs
    cols_attributes = [x for x in cols if x not in cols_ids + ["Sample_Type", "Biopsy_Type"]]
    df_sam_dna_n = df_sam.loc[df_sam["Sample_Type"]=="DNA_N"][cols_attributes + ["Sample_Id", "File_Size"]]
    df_sam_dna_n = df_sam_dna_n.rename(columns={"Sample_Id": "DNA_N", "File_Size": "File_Size_N"})

    df_sam_dna_t = df_sam.loc[df_sam["Sample_Type"]=="DNA_T"][cols_attributes + ["Sample_Id", "File_Size"]]
    df_sam_dna_t = df_sam_dna_t.rename(columns={"Sample_Id": "DNA_T", "File_Size": "File_Size_T"})
    df_tnp = df_sam_dna_t.merge(df_sam_dna_n, how="outer", on=cols_attributes)

    # drop samples with missing values
    df_tnp = df_tnp.dropna(how="any").copy()
    df_tnp["DNA_P"] = df_tnp[["DNA_T", "DNA_N"]].apply("_vs_".join, axis=1)
    df_tnp["File_Size_P"] = (df_tnp["File_Size_T"] + df_tnp["File_Size_N"])/1024**3

    # sort so that batch indices do not change from one run to another
    df_tnp = df_tnp.sort_values(by="DNA_P")

    # subsect to consider only pairs selected by MC3 consortium
    df_mc3 = pd.read_table(args.mc3_tnp)
    df_mc3["DNA_P"] = df_mc3[["Tumor_Sample_Barcode", "Matched_Norm_Sample_Barcode"]].apply("_vs_".join, axis=1)

    # create batches
    # some pairs were already processed during tests
    dna_p_batch_1 = ["TCGA-05-4244-01A-01D-1105-08_vs_TCGA-05-4244-10A-01D-1105-08",
                     "TCGA-05-4415-01A-22D-1855-08_vs_TCGA-05-4415-10A-01D-1855-08",
                     "TCGA-02-0003-01A-01D-1490-08_vs_TCGA-02-0003-10A-01D-1490-08",
                     "TCGA-YB-A89D-01A-12D-A36O-08_vs_TCGA-YB-A89D-10A-01D-A367-08"]
    df_tnp_a = df_tnp.loc[df_tnp["DNA_P"].isin(dna_p_batch_1)].copy()
    df_tnp_a["Batch"] = 1

    # create batches iteratively
    df_tnp_b = df_tnp.loc[~df_tnp["DNA_P"].isin(dna_p_batch_1)].copy()
    df_tnp_b["Batch"] = np.nan

    i_batch = 2
    cum_file_size = 0
    cum_batch_size = 0
    r_tnp_batches = []
    for i_tnp, r_tnp in df_tnp_b.iterrows():
        cum_file_size += r_tnp["File_Size_P"]
        cum_batch_size += 1
        if cum_file_size > args.max_disk_size or cum_batch_size > args.max_batch_size:
            cum_file_size = r_tnp["File_Size_P"]
            cum_batch_size = 1
            i_batch += 1
        r_tnp_batch = r_tnp.copy().to_dict()
        r_tnp_batch["Batch"] = i_batch
        r_tnp_batches.append(r_tnp_batch)

    df_tnp_b = pd.DataFrame(r_tnp_batches)

    # merge
    df_tnp = pd.concat((df_tnp_a, df_tnp_b), axis=0)

    # subselect sam table to consider only samples with matched normal
    df_sam = df_sam.loc[df_sam["Sample_Id"].isin(df_tnp["DNA_T"].tolist()+df_tnp["DNA_N"].tolist())]

    # # add batch index to sam table
    # df_tnp_batch_t = df_tnp[["DNA_T", "Batch"]].rename(columns={"DNA_T": "Sample_Id"})
    # df_tnp_batch_n = df_tnp[["DNA_N", "Batch"]].rename(columns={"DNA_N": "Sample_Id"})
    # df_tnp_batch = pd.concat((df_tnp_batch_t, df_tnp_batch_n)).drop_duplicates()
    # df_sam = df_sam.merge(df_tnp_batch, how="left", on="Sample_Id")

    # save table of samples
    df_sam.to_csv(args.out_sam, index=False, sep="\t")

    # save table of samples
    df_tnp.to_csv(args.out_tnp, index=False, sep="\t")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare samples table.")
    parser.add_argument("--mc3_tnp", type=str, help="Path to table of MC3-analyzed tumor/normal pairs.",
                        default="../../data/tcga/wes/summary/mc3_unfiltered_analyzed.tsv")
    parser.add_argument("--out_sam", type=str, help="Path to table of samples.", default="config/samples.tsv")
    parser.add_argument("--out_tnp", type=str, help="Path to table of tumor/normal pairs.",
                        default="config/tumor_normal_pairs.tsv")
    parser.add_argument("--max_disk_size", type=int, help="Max disk size available for one batch.",
                        default=190)
    parser.add_argument("--max_batch_size", type=int, help="Max number of tumor/normal pairs for one batch.",
                        default=4)
    args = parser.parse_args()

    main(args)
