# Upload results
rule upload_results:
    input:
        expand("%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_chr_arm/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_sum/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_table/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
                  get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_gene_calls_unfiltered/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_gene_calls_filtered/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/annotation/somatic_cna_oncokb/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/annotation/somatic_cna_civic/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na)
    output:
        touch("%s/upload.done" % L_FOLDER)
    log:
        "%s/upload/upload_results.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    benchmark:
        "%s/upload/upload_results.tsv" % B_FOLDER
    resources:
        mem_mb=1000,
        time_min=30
    threads: 2
    params:
        gs_res_bucket="%s/%s" % (config["gcloud"]["gs_res_bucket"], "results"),
        start_from=config["start_from"]
    shell:
        """
        python -u gcloud/buckets/populate_results_gs_bucket.py \
            --bucket_gs_uri {params.gs_res_bucket} \
            --start_from {params.start_from} &> {log}
        """
