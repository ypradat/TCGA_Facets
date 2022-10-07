####
#### Somatic purity/ploidy ####
####

# Aggregate all somatic purity and ploidy values.
rule somatic_ppy_aggregate:
    input:
        expand("%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz" % R_FOLDER,
            get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na)
    output:
        "%s/aggregate/somatic_ppy/somatic_ppy.tsv" % R_FOLDER
    benchmark:
        "%s/aggregate/somatic_ppy/somatic_ppy_aggregate.tsv" % B_FOLDER
    log:
        "%s/aggregate/somatic_ppy/somatic_ppy_aggregate.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    threads: 1
    resources:
        queue="shortq",
        mem_mb=16000,
        time_min=15
    shell:
        """
        python -u workflow/scripts/07.4_concatenate_ppy.py \
            --input {input} \
            --output {output} &> {log}
        """



####
#### Copy number variants ####
####

rule somatic_cna_filters_aggregate:
    input:
        expand("%s/calling/somatic_cnv_bed/{tsample}_vs_{nsample}.bed.gz" % R_FOLDER,
            get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na)
    output:
        "%s/aggregate/somatic_cna/somatic_calls_filters.tsv.gz" % R_FOLDER
    benchmark:
        "%s/aggregate/somatic_cna_filters/somatic_cna_filters_aggregate.tsv" % B_FOLDER
    log:
        "%s/aggregate/somatic_cna_filters/somatic_cna_filters_aggregate.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    params:
        threshold=config["params"]["cnv"]["calls_threshold"]
    threads: 1
    resources:
        queue="shortq",
        mem_mb=16000,
        time_min=60
    shell:
        """
        python -u workflow/scripts/07.5_concatenate_somatic_cnv.py \
            --cnv {input} \
            --threshold {params.threshold} \
            --output {output} &> {log}
        """



rule somatic_cna_aggregate:
    input:
        expand("%s/calling/somatic_cnv_calls/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
            get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na)
    output:
        "%s/aggregate/somatic_cna/somatic_calls.tsv.gz" % R_FOLDER
    benchmark:
        "%s/aggregate/somatic_cna/somatic_cna_aggregate.tsv" % B_FOLDER
    log:
        "%s/aggregate/somatic_cna/somatic_cna_aggregate.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    params:
        threshold=config["params"]["cnv"]["calls_threshold"]
    threads: 1
    resources:
        queue="shortq",
        mem_mb=16000,
        time_min=60
    shell:
        """
        python -u workflow/scripts/07.5_concatenate_somatic_cnv.py \
            --cnv {input} \
            --threshold {params.threshold} \
            --output {output} &> {log}
        """


rule somatic_cna_sum_aggregate:
    input:
        expand("%s/calling/somatic_cnv_sum/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
            get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na)
    output:
        "%s/aggregate/somatic_cna/somatic_calls_summary_statistics.tsv.gz" % R_FOLDER
    benchmark:
        "%s/aggregate/somatic_cna_sum/all_samples.tsv" % B_FOLDER
    log:
        "%s/aggregate/somatic_cna_sum/all_samples.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    threads: 1
    resources:
        queue="shortq",
        mem_mb=16000,
        time_min=60
    shell:
        """
        python -u workflow/scripts/07.1_concatenate_tables.py \
            --input {input} \
            --output {output} &> {log}
        """


rule somatic_cna_chr_arm_aggregate:
    input:
        chr_arm=expand("%s/calling/somatic_cnv_chr_arm/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
            get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        cnv_sum=expand("%s/calling/somatic_cnv_sum/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
            get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
        cln="config/tumor_normal_pairs.tsv",
        rules=config["params"]["cnv"]["chr_arm_rules"]
    output:
        "%s/aggregate/somatic_cna/somatic_calls_per_chr_arm.tsv.gz" % R_FOLDER
    benchmark:
        "%s/aggregate/somatic_cna_chr_arm/all_samples.tsv" % B_FOLDER
    log:
        "%s/aggregate/somatic_cna_chr_arm/all_samples.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    threads: 1
    resources:
        queue="shortq",
        mem_mb=16000,
        time_min=60
    shell:
        """
        python -u workflow/scripts/07.6_concatenate_somatic_cnv_chr_arm.py \
            --input_chr_arm {input.chr_arm} \
            --input_cnv_sum {input.cnv_sum} \
            --input_cln {input.cln} \
            --rules {input.rules} \
            --output {output} &> {log}
        """


rule somatic_cna_oncokb_aggregate:
    input:
        lambda w: get_input_concatenate(w, typ="cna", db="oncokb")
    output:
        "%s/aggregate/somatic_cna/somatic_calls_oncokb.tsv.gz" % R_FOLDER
    benchmark:
        "%s/aggregate/somatic_cna/somatic_cna_oncokb_aggregate.tsv" % B_FOLDER
    log:
        "%s/aggregate/somatic_cna/somatic_cna_oncokb_aggregate.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    threads: 1
    resources:
        queue="shortq",
        mem_mb=16000,
        time_min=60
    shell:
        """
        python -u workflow/scripts/07.1_concatenate_tables.py \
            --input {input} \
            --output {output} &> {log}
        """



# Aggregate all somatic civic-annotated MAF tables.
rule somatic_cna_civic_aggregate:
    input:
        lambda w: get_input_concatenate(w, typ="cna", db="civic")
    output:
        "%s/aggregate/somatic_cna/somatic_calls_civic.tsv.gz" % R_FOLDER
    benchmark:
        "%s/aggregate/somatic_cna/somatic_cna_civic_aggregate.tsv" % B_FOLDER
    log:
        "%s/aggregate/somatic_cna/somatic_cna_civic_aggregate.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    threads: 1
    resources:
        queue="shortq",
        mem_mb=16000,
        time_min=60
    shell:
        """
        python -u workflow/scripts/07.1_concatenate_tables.py \
            --input {input} \
            --output {output} &> {log}
        """


# Aggregate oncokb and civic mutation annotations.
rule somatic_cna_union_ann:
    input:
        civ="%s/aggregate/somatic_cna/somatic_calls_civic.tsv.gz" % R_FOLDER,
        okb="%s/aggregate/somatic_cna/somatic_calls_oncokb.tsv.gz" % R_FOLDER
    output:
        "%s/aggregate/somatic_cna/somatic_calls_union_ann.tsv.gz" % R_FOLDER
    conda:
        "../envs/python.yaml"
    benchmark:
        "%s/aggregate/somatic_cna/somatic_cna_union_ann.tsv" % B_FOLDER
    log:
        "%s/aggregate/somatic_cna/somatic_cna_union_ann.log" % L_FOLDER
    resources:
        partition="cpu_short",
        mem_mb=8000,
        time="00:15:00"
    threads: 1
    shell:
        """
        python -u workflow/scripts/07.2_concatenate_annotations.py \
            --civ {input.civ} \
            --okb {input.okb} \
            --cat cna \
            --output {output} &> {log}
        """
