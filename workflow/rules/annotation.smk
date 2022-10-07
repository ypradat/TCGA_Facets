####
#### Copy number variants ####
####


# Prepare CNA table for annotation by oncokb annotator
if config["params"]["oncokb"]["run_per_sample"]["cna"]:
    # prepare a table for each pair tsample_vs_nsample
    rule somatic_cna_oncokb_preprocess:
        input:
            cnv="%s/calling/somatic_cnv_calls/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
            cln="config/tumor_normal_pairs.tsv",
            gen="%s/cancerGeneList_oncokb_annotated.tsv" % config["params"]["oncokb"]["data_dir"]
        output:
            cna="%s/annotation/somatic_cna_oncokb_preprocess/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
            cln="%s/annotation/somatic_cna_oncokb_preprocess/clinical_{tsample}_vs_{nsample}.tsv" % R_FOLDER,
        benchmark:
            "%s/annotation/somatic_cna_oncokb_preprocess/{tsample}_vs_{nsample}.tsv" % B_FOLDER
        log:
            "%s/annotation/somatic_cna_oncokb_preprocess/{tsample}_vs_{nsample}.log" % L_FOLDER
        conda:
            "../envs/python.yaml"
        params:
            alteration_type="cnv"
        threads: 1
        resources:
            queue="shortq",
            mem_mb=5000,
            time_min=20
        shell:
            """
            python -u workflow/scripts/05.2_oncokb_preprocess.py \
                --table_alt {input.cnv} \
                --table_cln {input.cln} \
                --table_gen {input.gen} \
                --gen_gene_name "Hugo Symbol" \
                --alteration_type {params.alteration_type} \
                --output_alt {output.cna} \
                --output_cln {output.cln} &> {log}
            """
else:
    # prepare a table for all pairs tsample_vs_nsample
    rule somatic_cna_oncokb_preprocess:
        input:
            cnv=expand("%s/calling/somatic_cnv_calls/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
                      get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
            cln="config/tumor_normal_pairs.tsv",
            gen="%s/cancerGeneList_oncokb_annotated.tsv" % config["params"]["oncokb"]["data_dir"]
        output:
            cna="%s/annotation/somatic_cna_oncokb_preprocess/all_samples.tsv" % R_FOLDER,
            cln="%s/annotation/somatic_cna_oncokb_preprocess/clinical_all_samples.tsv" % R_FOLDER,
        benchmark:
            "%s/annotation/somatic_cna_oncokb_preprocess/all_samples.tsv" % B_FOLDER
        log:
            "%s/annotation/somatic_cna_oncokb_preprocess/all_samples.log" % L_FOLDER
        conda:
            "../envs/python.yaml"
        params:
            alteration_type="cnv"
        threads: 1
        resources:
            queue="shortq",
            mem_mb=5000,
            time_min=20
        shell:
            """
            python -u workflow/scripts/05.2_oncokb_preprocess.py \
                --table_alt {input.cnv} \
                --table_cln {input.cln} \
                --table_gen {input.gen} \
                --gen_gene_name "Hugo Symbol" \
                --alteration_type {params.alteration_type} \
                --output_alt {output.cna} \
                --output_cln {output.cln} &> {log}
            """


# Annotate cnas using CnaAnnotator from oncokb-annotator
rule somatic_cna_oncokb:
    input:
        cna="%s/annotation/somatic_cna_oncokb_preprocess/{sample_pair}.tsv" % R_FOLDER,
        cln="%s/annotation/somatic_cna_oncokb_preprocess/clinical_{sample_pair}.tsv" % R_FOLDER,
    output:
        "%s/annotation/somatic_cna_oncokb/{sample_pair}.tsv" % R_FOLDER
    benchmark:
        "%s/annotation/somatic_cna_oncokb/{sample_pair}.tsv" % B_FOLDER
    log:
        "%s/annotation/somatic_cna_oncokb/{sample_pair}.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    params:
        code_dir=config["params"]["oncokb"]["code_dir"],
        token=config["params"]["oncokb"]["token"],
    threads: 1
    resources:
        queue="shortq",
        mem_mb=5000,
        time_min=60
    shell:
        """
        python {params.code_dir}/CnaAnnotator.py \
            -i {input.cna} \
            -c {input.cln} \
            -b {params.token} \
            -o {output} &> {log}
        """


# Filter output of oncokb annotations.
rule somatic_cna_oncokb_filter:
    input:
        cna="%s/annotation/somatic_cna_oncokb/{sample_pair}.tsv" % R_FOLDER,
        rul=config["params"]["oncokb"]["rules_clean"]
    output:
        "%s/annotation/somatic_cna_oncokb_filter/{sample_pair}.tsv" % R_FOLDER
    benchmark:
        "%s/annotation/somatic_cna_oncokb_filter/{sample_pair}.tsv" % B_FOLDER
    log:
        "%s/annotation/somatic_cna_oncokb_filter/{sample_pair}.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    params:
        alteration_type="cnv"
    threads: 1
    resources:
        queue="shortq",
        mem_mb=5000,
        time_min=10
    shell:
        """
        python -u workflow/scripts/05.3_oncokb_postprocess.py \
            --input {input.cna} \
            --rules {input.rul} \
            --alteration_type {params.alteration_type} \
            --output {output} &> {log}
        """


# Prepare CNA table for annotation by (in-house) civic annotator
if config["params"]["civic"]["run_per_sample"]["cna"]:
    # prepare a table for each pair tsample_vs_nsample
    rule somatic_cna_civic_preprocess:
        input:
            cna="%s/calling/somatic_cnv_calls/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
            cln="config/tumor_normal_pairs.tsv",
            gen=config["params"]["civic"]["gene_list"]
        output:
            "%s/annotation/somatic_cna_civic_preprocess/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
        benchmark:
            "%s/annotation/somatic_cna_civic_preprocess/{tsample}_vs_{nsample}.tsv" % B_FOLDER
        log:
            "%s/annotation/somatic_cna_civic_preprocess/{tsample}_vs_{nsample}.log" % L_FOLDER
        conda:
            "../envs/python.yaml"
        params:
            alteration_type="cnv"
        threads: 1
        resources:
            queue="shortq",
            mem_mb=5000,
            time_min=20
        shell:
            """
            python -u workflow/scripts/05.2_civic_preprocess.py \
                --table_alt {input.cna} \
                --table_cln {input.cln} \
                --table_gen {input.gen} \
                --gen_gene_name name \
                --alteration_type {params.alteration_type} \
                --output {output} &> {log}
            """
else:
    # prepare a table for all pairs tsample_vs_nsample
    rule somatic_cna_civic_preprocess:
        input:
            cna=expand("%s/calling/somatic_cnv_calls/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
                      get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
            cln="config/tumor_normal_pairs.tsv",
            gen=config["params"]["civic"]["gene_list"]
        output:
            "%s/annotation/somatic_cna_civic_preprocess/all_samples.tsv" % R_FOLDER,
        benchmark:
            "%s/annotation/somatic_cna_civic_preprocess/all_samples.tsv" % B_FOLDER
        log:
            "%s/annotation/somatic_cna_civic_preprocess/all_samples.log" % L_FOLDER
        conda:
            "../envs/python.yaml"
        params:
            alteration_type="cnv"
        threads: 1
        resources:
            queue="shortq",
            mem_mb=5000,
            time_min=20
        shell:
            """
            python -u workflow/scripts/05.2_civic_preprocess.py \
                --table_alt {input.cna} \
                --table_cln {input.cln} \
                --table_gen {input.gen} \
                --gen_gene_name name \
                --alteration_type {params.alteration_type} \
                --output {output} &> {log}
            """


# Annotate cnas using (in-house) civic annotator
rule somatic_cna_civic:
    input:
        cna="%s/annotation/somatic_cna_civic_preprocess/{sample_pair}.tsv" % R_FOLDER,
        civ=config["params"]["civic"]["evidences"],
        rul=config["params"]["civic"]["rules_clean"]
    output:
        "%s/annotation/somatic_cna_civic/{sample_pair}.tsv" % R_FOLDER
    benchmark:
        "%s/annotation/somatic_cna_civic/{sample_pair}.tsv" % B_FOLDER
    log:
        "%s/annotation/somatic_cna_civic/{sample_pair}.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    params:
        code_dir=config["params"]["civic"]["code_dir"],
        category="cna"
    threads: 1
    resources:
        queue="shortq",
        mem_mb=5000,
        time_min=60
    shell:
        """
        python -u {params.code_dir}/civic.py \
            --input {input.cna} \
            --civic {input.civ} \
            --rules {input.rul} \
            --category {params.category} \
            --output {output} &> {log}
        """


# Filter output of civic annotations.
rule somatic_cna_civic_filter:
    input:
        "%s/annotation/somatic_cna_civic/{sample_pair}.tsv" % R_FOLDER
    output:
        "%s/annotation/somatic_cna_civic_filter/{sample_pair}.tsv" % R_FOLDER
    benchmark:
        "%s/annotation/somatic_cna_civic_filter/{sample_pair}.tsv" % B_FOLDER
    log:
        "%s/annotation/somatic_cna_civic_filter/{sample_pair}.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    threads: 1
    resources:
        queue="shortq",
        mem_mb=5000,
        time_min=10
    shell:
        """
        python -u workflow/scripts/05.3_civic_postprocess.py \
            --input {input} \
            --output {output} &> {log}
        """
