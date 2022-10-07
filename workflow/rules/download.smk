####
#### TCGA specific ####
####

# Download BAM files from manifests using gdc-client.
rule download_bam:
    log:
        "%s/mapping/gdc_get_bam_{sample}.log" % L_FOLDER
    benchmark:
        "%s/mapping/gdc_get_bam_{sample}.tsv" % B_FOLDER
    input:
        table = config["samples"]
    params:
        dir = "%s/mapping" % R_FOLDER,
        bam_name=lambda w: "%s/mapping/%s" % (R_FOLDER, get_column_table_sample(w, "File_Name")),
        bam_name_key=lambda w: "%s/mapping/%s" % (R_FOLDER, get_column_table_sample(w, "File_Name_Key")),
        index_bam_name=lambda w: "%s/mapping/%s" % (R_FOLDER, get_column_table_sample(w, "Index_File_Name")),
        index_bam_name_key=lambda w: "%s/mapping/%s" % (R_FOLDER, get_column_table_sample(w, "Index_File_Name_Key"))
    output:
        bam="%s/mapping/{sample}.bam" % R_FOLDER,
        bai="%s/mapping/{sample}.bai" % R_FOLDER,
    resources:
        mem_mb=1000,
        time_min=10
    threads: 4
    shell:
        """
        bash workflow/scripts/01.1_get_bam.sh \
            -d {params.dir} \
            -a {params.bam_name} \
            -b {params.bam_name_key} \
            -c {params.index_bam_name} \
            -d {params.index_bam_name_key} &> {log}
        """
