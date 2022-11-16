if config["start_from"]=="download_bam":
    # Get BAM files from the bucket tcga_wxs_bam
    # If this rule fails, or if the snakemake pipeline is restarted, this rule
    # will be skipped and the missing BAM files will be download by 01.2_get_snp_pileup.sh
    rule download_bam:
        log:
            "%s/mapping/download_bam/{sample}.log" % L_FOLDER
        benchmark:
            "%s/mapping/download_bam/{sample}.tsv" % B_FOLDER
        input:
            table=config["samples"]
        params:
            gs_bam_bucket=config["gcloud"]["gs_bam_bucket"],
            gs_res_bucket=config["gcloud"]["gs_res_bucket"],
            vm_bam_folder="%s/mapping" % R_FOLDER,
            l_folder=L_FOLDER
        output:
            touch("%s/mapping/download_bam_{sample}.done" % L_FOLDER)
        resources:
            mem_mb=1000,
            time_min=120,
            load=1
        threads: 1
        shell:
            """
            bash workflow/scripts/01.1_get_bam.sh \
                -a {params.gs_bam_bucket} \
                -b {params.gs_res_bucket} \
                -v {params.vm_bam_folder} \
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
        output:
            touch("%s/remove_bams.done" % L_FOLDER)
        params:
            gs_bucket=config["gcloud"]["gs_bam_bucket"]
        threads: 1
        resources:
            mem_mb=1000,
            time_min=20
        shell:
            """
            python -u gcloud/buckets/depopulate_bam_gs_bucket.py \
               --samples_table config/samples.tsv \
               --bucket_gs_uri {params.gs_bucket} &> {log}
            """

if config["start_from"] in ["download_bam", "get_snp_pileup", "somatic_cnv_facets"]:
    # Get snp pileup table
    rule get_snp_pileup:
        log:
            "%s/mapping/get_snp_pileup/{tsample}_vs_{nsample}.log" % L_FOLDER
        benchmark:
            "%s/mapping/get_snp_pileup/{tsample}_vs_{nsample}.tsv" % B_FOLDER
        conda:
            "../envs/main.yaml"
        input:
            get_input_snp_pileup
        output:
            snp_pileup="%s/calling/somatic_snp_pileup/{tsample}_vs_{nsample}.csv.gz" % R_FOLDER,
            nbhd_snp="%s/calling/somatic_nbhd_snp/{tsample}_vs_{nsample}.tsv" % R_FOLDER
        params:
            gs_bam_bucket=config["gcloud"]["gs_bam_bucket"],
            gs_snp_bucket="%s/results" % config["gcloud"]["gs_res_bucket"],
            vm_res_folder=R_FOLDER
        threads:
            get_threads_snp_pileup
        resources:
            queue="shortq",
            mem_mb=28000,
            time_min=60,
            load=get_load_snp_pileup
        shell:
            """
            bash workflow/scripts/01.2_get_snp_pileup.sh \
                -a {params.gs_bam_bucket} \
                -b {params.gs_snp_bucket} \
                -r {params.vm_res_folder} \
                -t {wildcards.tsample} \
                -n {wildcards.nsample} \
                -v {input[1]} \
                -p {threads} &> {log}
            """
