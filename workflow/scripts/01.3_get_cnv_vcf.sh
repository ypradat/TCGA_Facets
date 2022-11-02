#!/bin/bash

while getopts ":a:b:t:n:" opt; do
    case $opt in
	a) gs_res_bucket="$OPTARG"
	    ;;
	b) vm_res_bucket="$OPTARG"
	    ;;
	t) tsample="$OPTARG"
	    ;;
	n) nsample="$OPTARG"
	    ;;
	\?) echo "Invalid option -$OPTARG" >&2
	    exit 1
	    ;;
    esac

    case $OPTARG in
	-*) echo "Option $opt needs a valid argument"
	    exit 1
	    ;;
    esac
done

gs_vcf_file=${gs_bam_bucket}/calling/somatic_cnv_facets/${tsample}_vs_${nsample}.vcf.gz
vm_vcf_file=${vm_res_folder}/calling/somatic_cnv_facets/${tsample}_vs_${nsample}.vcf.gz
gsutil ls ${gs_vcf_file}
status=$?

if [[ $status != 0 ]]; then
  printf -- "-ERROR: vcf file does not exist and/or cannot be downloaded\n" "${gs_vcf_file}"
else
  gsutil cp ${gs_vcf_file} ${vm_vcf_file}
fi
