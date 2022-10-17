rule setup_r:
    log:
        "%s/setup.log" % L_FOLDER
    conda:
        "../envs/r.yaml"
    output:
        touch("%s/setup_r.done" % L_FOLDER)
    resources:
        queue="shortq",
        mem_mb=16000,
        time_min=60
    shell:
        """
        Rscript -e 'devtools::install_github("mskcc/facets-suite")' &> {log}
        """


rule setup_main:
    log:
        "%s/setup.log" % L_FOLDER
    conda:
        "../envs/main.yaml"
    output:
        touch("%s/setup_main.done" % L_FOLDER)
    resources:
        queue="shortq",
        mem_mb=2000,
        time_min=60
    shell:
        """
        Rscript -e 'devtools::install_github("https://github.com/veseshan/pctGCdata")' &> {log}
        Rscript -e 'devtools::install_github("https://github.com/ypradat/facets")' &>> {log}
        """
