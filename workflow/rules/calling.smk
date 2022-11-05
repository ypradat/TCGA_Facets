####
#### Copy number variants ####
####

if config["start_from"] in ["download_bam", "get_snp_pileup", "somatic_cnv_facets"]:
    # Call CNV using cnv-facets which is a wrapper around facets
    # See https://github.com/dariober/cnv_facets
    # Tumor-normal mode
    rule somatic_cnv_facets_tumor_normal:
        wildcard_constraints:
            tsample="|".join([re.escape(x) for x in tsamples]),
            nsample="|".join([re.escape(x) for x in nsamples])
        input:
            vcf=config["params"]["gatk"]["known_sites"],
            snp_pileup="%s/calling/somatic_snp_pileup/{tsample}_vs_{nsample}.csv.gz" % R_FOLDER,
            nbhd_snp="%s/calling/somatic_nbhd_snp/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
            env="%s/setup_main.done" % L_FOLDER
        output:
            vcf="%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz" % R_FOLDER,
            tbi="%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz.tbi" % R_FOLDER,
            png="%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.cnv.png" % R_FOLDER,
            cov="%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.cov.pdf" % R_FOLDER,
            spider="%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.spider.pdf" % R_FOLDER
        benchmark:
            "%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.tsv" % B_FOLDER
        log:
            "%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.log" % L_FOLDER
        conda:
            "../envs/main.yaml"
        params:
            prefix="{tsample}_vs_{nsample}",
            nbhd_snp=lambda wildcards, input: pd.read_table(input.nbhd_snp)["nbhd_snp"].get(0),
            cval_pre=config["params"]["cnv"]["facets"]["cvals"]["pre"],
            cval_pro=config["params"]["cnv"]["facets"]["cvals"]["pro"],
            gbuild=config["params"]["cnv"]["facets"]["gbuild"]
        threads: 1
        resources:
            queue="shortq",
            mem_mb=28000,
            time_min=90,
            load=get_load_snp_pileup
        shell:
            """
            Rscript external/cnv_facets/bin/cnv_facets.R \
                -vcf {input.vcf} \
                -p {input.snp_pileup} \
                -snp {params.nbhd_snp} \
                -o {params.prefix} \
                -N {threads} \
                --gbuild {params.gbuild} \
                --cval {params.cval_pre} {params.cval_pro} &> {log} && \
            mv {wildcards.tsample}_vs_{wildcards.nsample}.vcf.gz {output.vcf} && \
            mv {wildcards.tsample}_vs_{wildcards.nsample}.vcf.gz.tbi {output.tbi} && \
            mv {wildcards.tsample}_vs_{wildcards.nsample}.cnv.png {output.png} && \
            mv {wildcards.tsample}_vs_{wildcards.nsample}.cov.pdf {output.cov} && \
            mv {wildcards.tsample}_vs_{wildcards.nsample}.spider.pdf {output.spider}
        """

if config["start_from"] in ["somatic_cnv_process_vcf"]:
    # Get snp pileup table
    rule get_cnv_vcf:
        log:
            "%s/calling/get_cnv_vcf_{tsample}_vs_{nsample}.log" % L_FOLDER
        benchmark:
            "%s/calling/get_cnv_vcf_{tsample}_vs_{nsample}.tsv" % B_FOLDER
        conda:
            "../envs/main.yaml"
        output:
            vcf="%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz" % R_FOLDER,
        params:
            gs_res_bucket="%s/%s" % (config["gcloud"]["gs_res_bucket"], "results"),
            vm_res_folder=R_FOLDER
        threads: 1
        resources:
            queue="shortq",
            mem_mb=2000,
            time_min=15,
            load=1
        shell:
            """
            bash workflow/scripts/01.3_get_cnv_vcf.sh \
                -a {params.gs_res_bucket} \
                -b {params.vm_res_folder} \
                -t {wildcards.tsample} \
                -n {wildcards.nsample} &> {log}
            """

# Process VCF file
rule somatic_cnv_process_vcf:
    input:
        vcf="%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz" % R_FOLDER,
        rules_arm=config["params"]["cnv"]["chr_arm_rules"],
        rules_cat=config["params"]["cnv"]["cna_cat_rules"],
        env="%s/setup_r.done" % L_FOLDER
    output:
        arm="%s/calling/somatic_cnv_chr_arm/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
        sum="%s/calling/somatic_cnv_sum/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
        tab="%s/calling/somatic_cnv_table/{tsample}_vs_{nsample}.tsv" % R_FOLDER
    benchmark:
        "%s/calling/somatic_cnv_process_vcf/{tsample}_vs_{nsample}.tsv" % B_FOLDER
    log:
        "%s/calling/somatic_cnv_process_vcf/{tsample}_vs_{nsample}.log" % L_FOLDER
    conda:
        "../envs/r.yaml"
    threads: 1
    params:
        gender = lambda w: get_column_table_sample(w, "Gender")
    resources:
        queue="shortq",
        mem_mb=8000,
        time_min=20
    shell:
        """
        Rscript workflow/scripts/02.1_cnv_process_vcf.R \
            --input_vcf {input.vcf} \
            --gender {params.gender} \
            --rules_arm {input.rules_arm} \
            --rules_cat {input.rules_cat} \
            --output_arm {output.arm} \
            --output_sum {output.sum} \
            --output_tab {output.tab} \
            --log {log}
        """


# Convert table with cnv at segments to cnv at genes using bedtools
rule somatic_cnv_gene_calls:
    input:
        tab="%s/calling/somatic_cnv_table/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
        bed=config["params"]["cnv"]["bed"]
    output:
        "%s/calling/somatic_cnv_gene_calls/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER
    benchmark:
        "%s/calling/somatic_cnv_gene_calls/{tsample}_vs_{nsample}.tsv" % B_FOLDER
    log:
        "%s/calling/somatic_cnv_gene_calls/{tsample}_vs_{nsample}.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    params:
        threshold=config["params"]["cnv"]["calls_threshold"]
    threads: 1
    resources:
        queue="shortq",
        mem_mb=8000,
        time_min=20
    shell:
        """
        python -u workflow/scripts/02.2_cnv_gene_calls.py \
            --input_tab {input.tab} \
            --input_bed {input.bed} \
            --threshold {params.threshold} \
            --output {output} &> {log}
        """


# Convert table with cnv at segments to cnv at genes using bedtools
rule somatic_cnv_gene_calls_filtered:
    input:
        "%s/calling/somatic_cnv_gene_calls/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER
    output:
        temp("%s/calling/somatic_cnv_gene_calls_filtered/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER)
    benchmark:
        "%s/calling/somatic_cnv_gene_calls_filtered/{tsample}_vs_{nsample}.tsv" % B_FOLDER
    log:
        "%s/calling/somatic_cnv_gene_calls_filtered/{tsample}_vs_{nsample}.log" % L_FOLDER
    threads: 1
    resources:
        queue="shortq",
        mem_mb=8000,
        time_min=20
    shell:
        """
        zcat {input} | grep "PASS\|Tumor_Sample_Barcode" | gzip > {output} 2> {log}
        """
