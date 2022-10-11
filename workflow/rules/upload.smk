# Upload results
rule upload_results:
    log:
        "%s/mapping/upload_results.log" % L_FOLDER
    benchmark:
        "%s/mapping/upload_results.tsv" % B_FOLDER
    input:
        expand("%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_table/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
                  get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_bed/{tsample}_vs_{nsample}.bed.gz" % R_FOLDER,
                  get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_calls/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_chr_arm/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/calling/somatic_cnv_sum/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/annotation/somatic_cna_oncokb_filter/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        expand("%s/annotation/somatic_cna_civic_filter/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
               get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
    params:
        gs_bucket = "gs://facets_tcga_results"
    output:
        touch("%s/upload.done" % L_FOLDER)
    resources:
        mem_mb=1000,
        time_min=30
    threads: 2
    shell:
        """
        python -u workflow/scripts/populate_results_gs_bucket.py \
            --bucket_gs_uri {params.gs_bucket} &> {log}
        """
