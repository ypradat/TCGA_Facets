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
        gs_bucket = "gs://tcga_wxs_bam",
        local_dir = "%s/mapping" % R_FOLDER,
    output:
        bam="%s/mapping/{sample}.bam" % R_FOLDER,
        bai="%s/mapping/{sample}.bai" % R_FOLDER,
    resources:
        mem_mb=1000,
        time_min=120
    threads: 2
    shell:
        """
        bash workflow/scripts/01.1_get_bam.sh \
            -b {params.gs_bucket} \
            -d {params.local_dir} \
            -s {wildcards.sample} &> {log}
        """
