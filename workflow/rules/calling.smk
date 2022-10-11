####
#### Copy number variants ####
####


# Call CNV using cnv-facets which is a wrapper around facets
# See https://github.com/dariober/cnv_facets
# Tumor-normal mode
rule somatic_cnv_facets_tumor_normal:
    wildcard_constraints:
        tsample = "|".join([re.escape(x) for x in tsamples]),
        nsample = "|".join([re.escape(x) for x in nsamples])
    input:
        vcf=config["params"]["gatk"]["known_sites"],
        tbam="%s/mapping/{tsample}.bam" % R_FOLDER,
        tbai="%s/mapping/{tsample}.bai" % R_FOLDER,
        nbam="%s/mapping/{nsample}.bam" % R_FOLDER,
        nbai="%s/mapping/{nsample}.bai" % R_FOLDER
    output:
        vcf="%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz" % R_FOLDER,
        tbi="%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz.tbi" % R_FOLDER,
        png="%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.cnv.png" % R_FOLDER,
        cov="%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.cov.pdf" % R_FOLDER,
        spider="%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.spider.pdf" % R_FOLDER,
        pileup=temp("%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.csv.gz" % R_FOLDER)
    benchmark:
        "%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.tsv" % B_FOLDER
    log:
        "%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.log" % L_FOLDER
    conda:
        "../envs/main.yaml"
    params:
        prefix="{tsample}_vs_{nsample}",
        cval_pre=config["params"]["cnv"]["facets"]["cvals"]["pre"],
        cval_pro=config["params"]["cnv"]["facets"]["cvals"]["pro"],
        gbuild=config["params"]["cnv"]["facets"]["gbuild"],
        dir="%s/calling/somatic_cnv_facets" % R_FOLDER
    threads: 10
    resources:
        queue="shortq",
        mem_mb=64000,
        time_min=90
    shell:
        """
        cnv_facets.R \
            -vcf {input.vcf} \
            -t {input.tbam} \
            -n {input.nbam} \
            -o {params.prefix} \
            -N {threads} \
            --gbuild {params.gbuild} \
            --cval {params.cval_pre} {params.cval_pro} &> {log} && \
        mv {wildcards.tsample}_vs_{wildcards.nsample}.vcf.gz {output.vcf} && \
        mv {wildcards.tsample}_vs_{wildcards.nsample}.vcf.gz.tbi {output.tbi} && \
        mv {wildcards.tsample}_vs_{wildcards.nsample}.cnv.png {output.png} && \
        mv {wildcards.tsample}_vs_{wildcards.nsample}.cov.pdf {output.cov} && \
        mv {wildcards.tsample}_vs_{wildcards.nsample}.spider.pdf {output.spider} && \
        mv {wildcards.tsample}_vs_{wildcards.nsample}.csv.gz {output.pileup}
	"""


# Convert VCF file to tsv file
rule somatic_cnv_table:
    input:
        "%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz" % R_FOLDER
    output:
        "%s/calling/somatic_cnv_table/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER
    benchmark:
        "%s/calling/somatic_cnv_table/{tsample}_vs_{nsample}.tsv" % B_FOLDER
    log:
        "%s/calling/somatic_cnv_table/{tsample}_vs_{nsample}.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    threads: 1
    params:
        gender = lambda w: get_column_table_sample(w, "Gender")
    resources:
        queue="shortq",
        mem_mb=8000,
        time_min=20
    shell:
        """
        python -u workflow/scripts/06.1_cnv_vcf_to_table.py \
            --input {input} \
            --gender {params.gender} \
            --output {output} &> {log}
        """


# Convert table with cnv at segments to cnv at genes using bedtools
rule somatic_cnv_bed:
    input:
        tab="%s/calling/somatic_cnv_table/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
        bed=config["params"]["cnv"]["bed"]
    output:
        "%s/calling/somatic_cnv_bed/{tsample}_vs_{nsample}.bed.gz" % R_FOLDER
    benchmark:
        "%s/calling/somatic_cnv_bed/{tsample}_vs_{nsample}.tsv" % B_FOLDER
    log:
        "%s/calling/somatic_cnv_bed/{tsample}_vs_{nsample}.log" % L_FOLDER
    conda:
        "../envs/python.yaml"
    threads: 1
    resources:
        queue="shortq",
        mem_mb=8000,
        time_min=20
    shell:
        """
        python -u workflow/scripts/06.2_cnv_table_to_bed.py \
            --input_tab {input.tab} \
            --input_bed {input.bed} \
            --output {output} &> {log}
        """


# Convert table with cnv at segments to cnv at genes using bedtools
rule somatic_cnv_chr_arm_sum:
    input:
        tab="%s/calling/somatic_cnv_table/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER,
        vcf="%s/calling/somatic_cnv_facets/{tsample}_vs_{nsample}.vcf.gz" % R_FOLDER,
        env="%s/setup_r.done" % L_FOLDER
    output:
        arm="%s/calling/somatic_cnv_chr_arm/{tsample}_vs_{nsample}.tsv" % R_FOLDER,
        sum="%s/calling/somatic_cnv_sum/{tsample}_vs_{nsample}.tsv" % R_FOLDER
    benchmark:
        "%s/calling/somatic_cnv_chr_arm_sum/{tsample}_vs_{nsample}.tsv" % B_FOLDER
    log:
        "%s/calling/somatic_cnv_chr_arm_sum/{tsample}_vs_{nsample}.log" % L_FOLDER
    conda:
        "../envs/r.yaml"
    threads: 1
    params:
        genome=config["params"]["cnv"]["facets"]["gbuild"]
    resources:
        queue="shortq",
        mem_mb=8000,
        time_min=20
    shell:
        """
        Rscript workflow/scripts/06.3_cnv_chr_arm_sum.R \
            --input_tab {input.tab} \
            --input_vcf {input.vcf} \
            --genome {params.genome} \
            --output_arm {output.arm} \
            --output_sum {output.sum} \
            --log {log}
        """


# Make a table of filter cnv calls per gene
rule somatic_cnv_calls:
    input:
        "%s/calling/somatic_cnv_bed/{tsample}_vs_{nsample}.bed.gz" % R_FOLDER
    output:
        "%s/calling/somatic_cnv_calls/{tsample}_vs_{nsample}.tsv.gz" % R_FOLDER
    benchmark:
        "%s/calling/somatic_cnv_calls/{tsample}_vs_{nsample}.tsv" % B_FOLDER
    log:
        "%s/calling/somatic_cnv_calls/{tsample}_vs_{nsample}.log" % L_FOLDER
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
        python -u workflow/scripts/06.4_cnv_filter_calls.py \
            --input_bed {input} \
            --threshold {params.threshold} \
            --output {output} &> {log}
        """
