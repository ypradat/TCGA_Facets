####
#### Copy number variants ####
####

# Annnotate SCNAs using (in-house) civic annotator
if config["params"]["civic"]["run_per_sample"]["cna"]:
    # prepare a table for each pair tsample_vs_nsample
    rule somatic_cna_civic:
        input:
            table_alt="%s/calling/somatic_cnv_gene_calls_filtered/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
            table_cln="config/tumor_normal_pairs.tsv",
            table_gen=config["params"]["civic"]["gene_list"],
            civic=config["params"]["civic"]["evidences"],
            rules=config["params"]["civic"]["rules_clean"]
        output:
            table_pre=temp("%s/annotation/somatic_cna_civic/{tsample}_vs_{nsample}_pre.tsv" % R_FOLDER),
            table_run=temp("%s/annotation/somatic_cna_civic/{tsample}_vs_{nsample}_run.tsv" % R_FOLDER),
            table_pos="%s/annotation/somatic_cna_civic/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
        benchmark:
            "%s/annotation/somatic_cna_civic/{tsample}_vs_{nsample}.tsv" % B_FOLDER
        log:
            "%s/annotation/somatic_cna_civic/{tsample}_vs_{nsample}.log" % L_FOLDER
        conda:
            "../envs/python.yaml"
        params:
            code_dir=config["params"]["civic"]["code_dir"],
            category="cna",
            a_option=lambda wildcards, input: "-a %s" % input.table_alt
        threads: 1
        resources:
            queue="shortq",
            mem_mb=4000,
            time_min=20
        shell:
            """
            bash workflow/scripts/03.3_civic_annotate.sh \
                {params.a_option} \
                -b {input.table_cln} \
                -c {input.table_gen} \
                -d {output.table_pre} \
                -e {output.table_run} \
                -f {output.table_pos} \
                -m {params.code_dir} \
                -n {input.civic} \
                -o {input.rules} \
                -t {params.category} \
                -l {log}
            """
else:
    # prepare a table for all pairs tsample_vs_nsample
    rule somatic_cna_civic:
        input:
            table_alt=expand("%s/calling/somatic_cnv_gene_calls_filtered/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
                      get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
            table_cln="config/tumor_normal_pairs.tsv",
            table_gen=config["params"]["civic"]["gene_list"],
            civic=config["params"]["civic"]["evidences"],
            rules=config["params"]["civic"]["rules_clean"]
        output:
            table_pre=temp("%s/annotation/somatic_cna_civic/all_samples_pre.tsv" % R_FOLDER),
            table_run=temp("%s/annotation/somatic_cna_civic/all_samples_run.tsv" % R_FOLDER),
            table_pos="%s/annotation/somatic_cna_civic/all_samples.tsv" % R_FOLDER,
        benchmark:
            "%s/annotation/somatic_cna_civic/all_samples.tsv" % B_FOLDER
        log:
            "%s/annotation/somatic_cna_civic/all_samples.log" % L_FOLDER
        conda:
            "../envs/python.yaml"
        params:
            code_dir=config["params"]["civic"]["code_dir"],
            category="cna",
            a_option=lambda wildcards, input: "-a " + " -a ".join(input.table_alt)
        threads: 1
        resources:
            queue="shortq",
            mem_mb=24000,
            time_min=90
        shell:
            """
            bash workflow/scripts/03.3_civic_annotate.sh \
                {params.a_option} \
                -b {input.table_cln} \
                -c {input.table_gen} \
                -d {output.table_pre} \
                -e {output.table_run} \
                -f {output.table_pos} \
                -m {params.code_dir} \
                -n {input.civic} \
                -o {input.rules} \
                -t {params.category} \
                -l {log}
            """


# Annnotate SCNAs using oncokb annotator
if config["params"]["oncokb"]["run_per_sample"]["cna"]:
    # prepare a table for each pair tsample_vs_nsample
    rule somatic_cna_oncokb:
        input:
            table_alt="%s/calling/somatic_cnv_gene_calls_filtered/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
            table_cln="config/tumor_normal_pairs.tsv",
            table_gen=config["params"]["oncokb"]["gene_list"],
            rules=config["params"]["oncokb"]["rules_clean"]
        output:
            table_alt_pre=temp("%s/annotation/somatic_cna_oncokb/{tsample}_vs_{nsample}_alt_pre.tsv" % R_FOLDER),
            table_cln_pre=temp("%s/annotation/somatic_cna_oncokb/{tsample}_vs_{nsample}_cln_pre.tsv" % R_FOLDER),
            table_run=temp("%s/annotation/somatic_cna_oncokb/{tsample}_vs_{nsample}_run.tsv" % R_FOLDER),
            table_pos="%s/annotation/somatic_cna_oncokb/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
        benchmark:
            "%s/annotation/somatic_cna_oncokb/{tsample}_vs_{nsample}.tsv" % B_FOLDER
        log:
            "%s/annotation/somatic_cna_oncokb/{tsample}_vs_{nsample}.log" % L_FOLDER
        conda:
            "../envs/python.yaml"
        params:
            token=config["params"]["oncokb"]["token"],
            code_dir=config["params"]["oncokb"]["code_dir"],
            category="cna",
            a_option=lambda wildcards, input: "-a %s" % input.table_alt
        threads: 1
        resources:
            queue="shortq",
            mem_mb=4000,
            time_min=20
        shell:
            """
            bash workflow/scripts/03.3_oncokb_annotate.sh \
                {params.a_option} \
                -b {input.table_cln} \
                -c {input.table_gen} \
                -d {output.table_alt_pre} \
                -g {output.table_cln_pre} \
                -e {output.table_run} \
                -f {output.table_pos} \
                -k {params.token} \
                -m {params.code_dir} \
                -o {input.rules} \
                -t {params.category} \
                -l {log}
            """
else:
    # prepare a table for all pairs tsample_vs_nsample
    rule somatic_cna_oncokb:
        input:
            table_alt=expand("%s/calling/somatic_cnv_gene_calls_filtered/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
                      get_allowed_pairs_tumor_normal(), tsample=tsamples, nsample=nsamples_na),
            table_cln="config/tumor_normal_pairs.tsv",
            table_gen=config["params"]["oncokb"]["gene_list"],
            rules=config["params"]["oncokb"]["rules_clean"]
        output:
            table_alt_pre=temp("%s/annotation/somatic_cna_oncokb/all_samples_alt_pre.tsv" % R_FOLDER),
            table_cln_pre=temp("%s/annotation/somatic_cna_oncokb/all_samples_cln_pre.tsv" % R_FOLDER),
            table_run=temp("%s/annotation/somatic_cna_oncokb/all_samples_run.tsv" % R_FOLDER),
            table_pos="%s/annotation/somatic_cna_oncokb/all_samples.tsv" % R_FOLDER,
        benchmark:
            "%s/annotation/somatic_cna_oncokb/all_samples.tsv" % B_FOLDER
        log:
            "%s/annotation/somatic_cna_oncokb/all_samples.log" % L_FOLDER
        conda:
            "../envs/python.yaml"
        params:
            token=config["params"]["oncokb"]["token"],
            code_dir=config["params"]["oncokb"]["code_dir"],
            category="cna",
            a_option=lambda wildcards, input: "-a " + " -a ".join(input.table_alt)
        threads: 1
        resources:
            queue="shortq",
            mem_mb=4000,
            time_min=20
        shell:
            """
            bash workflow/scripts/03.3_oncokb_annotate.sh \
                {params.a_option} \
                -b {input.table_cln} \
                -c {input.table_gen} \
                -d {output.table_alt_pre} \
                -g {output.table_cln_pre} \
                -e {output.table_run} \
                -f {output.table_pos} \
                -k {params.token} \
                -m {params.code_dir} \
                -o {input.rules} \
                -t {params.category} \
                -l {log}
            """
