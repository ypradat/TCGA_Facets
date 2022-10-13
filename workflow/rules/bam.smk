# Get BAM files from the bucket tcga_wxs_bam
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
        time_min=120,
        load=10
    threads: 1
    shell:
        """
        bash workflow/scripts/01.1_get_bam.sh \
            -b {params.gs_bucket} \
            -d {params.local_dir} \
            -s {wildcards.sample} &> {log}
        """


# Remove BAM files from the bucket tcga_wxs_bam
rule remove_bams:
    log:
        "%s/mapping/remove_bams.log" % L_FOLDER
    benchmark:
        "%s/mapping/remove_bams.tsv" % B_FOLDER
    input:
        expand("%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
    params:
        gs_bucket = "gs://tcga_wxs_bam"
    output:
        touch("%s/remove_bams.done" % L_FOLDER)
    resources:
        mem_mb=1000,
        time_min=20
    threads: 1
    shell:
        """
        python -u gcloud/depopulate_bam_gs_bucket.py \
           --samples_table config/samples.tsv \
           --bucket_gs_uri {params.gs_bucket} &> {log}
        """
