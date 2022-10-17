#!/bin/bash

while getopts ":b:d:s:" opt; do
    case $opt in
	b) gs_bucket="$OPTARG"
	    ;;
	d) vm_folder="$OPTARG"
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

gs_bam_file=${gs_bucket}/${sample}.bam
gs_bai_file=${gs_bucket}/${sample}.bai

vm_bam_file=${vm_folder}/${sample}.bam
vm_bai_file=${vm_folder}/${sample}.bai

gsutil cp ${gs_bam_file} ${vm_bam_file}
gsutil cp ${gs_bai_file} ${vm_bai_file}
