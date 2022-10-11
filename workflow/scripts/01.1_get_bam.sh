#!/bin/bash

while getopts ":b:d:s:" opt; do
    case $opt in
	b) gs_bucket="$OPTARG"
	    ;;
	d) local_dir="$OPTARG"
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

local_bam_file=${local_dir}/${sample}.bam
local_bai_file=${local_dir}/${sample}.bai

gsutil cp ${gs_bam_file} ${local_bam_file}
gsutil cp ${gs_bai_file} ${local_bai_file}
