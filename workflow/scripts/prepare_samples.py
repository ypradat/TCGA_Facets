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
    df_sam["File_Name"] = df_sam["File_Name_Key"].apply(lambda x: x.split("/")[-1])[0]
    df_sam["Index_File_Name"] = df_sam["Index_File_Name_Key"].apply(lambda x: x.split("/")[-1])[0]
    df_sam["File_Size"] = df_sam["File_Size"].astype(int)
    df_sam["Index_File_Size"] = df_sam["Index_File_Size"].astype(int)

    # save table of samples
    df_sam.to_csv(args.out_sam, index=False, sep="\t")

    # prepare table of tumor/normal pairs
    cols_attributes = [x for x in cols if x not in cols_ids + ["Sample_Type", "Biopsy_Type"]]
    df_sam_dna_n = df_sam.loc[df_sam["Sample_Type"]=="DNA_N"][cols_attributes + ["Sample_Id"]]
    df_sam_dna_n = df_sam_dna_n.rename(columns={"Sample_Id": "DNA_N"})

    df_sam_dna_t = df_sam.loc[df_sam["Sample_Type"]=="DNA_T"][cols_attributes + ["Sample_Id"]]
    df_sam_dna_t = df_sam_dna_t.rename(columns={"Sample_Id": "DNA_T"})
    df_tnp = df_sam_dna_t.merge(df_sam_dna_n, how="outer", on=cols_attributes)

    # drop samples with missing values
    df_tnp = df_tnp.dropna(how="any")
    df_tnp["DNA_P"] = df_tnp[["DNA_T", "DNA_N"]].apply("_vs_".join, axis=1)

    # save table of samples
    df_tnp.to_csv(args.out_tnp, index=False, sep="\t")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prepare samples table.")
    parser.add_argument("--out_sam", type=str, help="Path to table of samples.", default="config/samples.tsv")
    parser.add_argument("--out_tnp", type=str, help="Path to table of tumor/normal pairs.",
                        default="config/tumor_normal_pairs.tsv")
    args = parser.parse_args()

    main(args)
