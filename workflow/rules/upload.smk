# Upload results for one  pair tumor/normal.
rule upload_pair_results:
    input:
        "%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz" % R_FOLDER,
        "%s/calling/somatic_cnv_chr_arm/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
        "%s/calling/somatic_cnv_sum/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
        "%s/calling/somatic_cnv_table/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
        "%s/calling/somatic_cnv_gene_calls/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
        "%s/annotation/somatic_cna_oncokb/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
        "%s/annotation/somatic_cna_civic/{tsample}_vs_{nsample}.tsv" % R_FOLDER
    output:
        touch("%s/upload_{tsample}_vs_{nsample}.done" % L_FOLDER)
    log:
        "%s/upload/upload_{tsample}_vs_{nsample}.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    benchmark:
        "%s/upload/upload_{tsample}_vs_{nsample}.tsv" % B_FOLDER
    resources:
        mem_mb=1000,
        time_min=60
    threads: 1
    params:
        gs_res_bucket=config["gcloud"]["gs_res_bucket"],
        start_from=config["start_from"]
    shell:
        """
        python -u gcloud/buckets/populate_results_gs_bucket.py \
            --bucket_gs_uri {params.gs_res_bucket} \
            --start_from {params.start_from} \
            --tsample {wildcards.tsample} \
            --nsample {wildcards.nsample} &> {log}
        """

# Upload log VM
rule upload_vm_log:
    input:
        expand("%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_chr_arm/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_sum/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_table/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
                  get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_gene_calls/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/annotation/somatic_cna_oncokb/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/annotation/somatic_cna_civic/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na)
    output:
        touch("%s/upload_vm_log.done" % L_FOLDER)
    log:
        "%s/upload/upload_vm_log.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    benchmark:
        "%s/upload/upload_vm_log.tsv" % B_FOLDER
    resources:
        mem_mb=1000,
        time_min=60
    threads: 1
    params:
        gs_res_bucket=config["gcloud"]["gs_res_bucket"],
        start_from=config["start_from"]
    shell:
        """
        python -u gcloud/buckets/populate_results_gs_bucket.py \
            --bucket_gs_uri {params.gs_res_bucket} \
            --start_from {params.start_from} \
            --vm_log &> {log}
        """
