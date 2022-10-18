#!/bin/bash

while getopts ":a:b:v:s:" opt; do
    case $opt in
	a) gs_bam_bucket="$OPTARG"
	    ;;
	b) gs_res_bucket="$OPTARG"
	    ;;
	v) vm_bam_folder="$OPTARG"
	    ;;
	s) sample="$OPTARG"
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

gs_bam_file=${gs_bam_bucket}/${sample}.bam
gs_bai_file=${gs_bam_bucket}/${sample}.bai

gs_snp_pileup=${gs_snp_bucket}/results/calling/somatic_snp_pileup/*${sample}*
gsutil ls ${gs_snp_pileup}
status=$?

if [[ $status != 0 ]]; then
  printf -- "-INFO: no snp-pileup file for sample %s, download the BAM file\n" "${sample}"

  vm_bam_file=${vm_bam_folder}/${sample}.bam
  vm_bai_file=${vm_bam_folder}/${sample}.bai

  gsutil cp ${gs_bam_file} ${vm_bam_file}
  gsutil cp ${gs_bai_file} ${vm_bai_file}
else
  printf -- "-INFO: snp-pileup file(s) found for for sample %s, do not download the BAM file\n" "${sample}"
fi
