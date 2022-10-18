#!/bin/bah

while getopts ":a:b:r:t:n:v:p:" opt; do
  case $opt in
    a) gs_bam_bucket="$OPTARG"
      ;;
    b) gs_snp_bucket="$OPTARG"
      ;;
    r) vm_res_folder="$OPTARG"
      ;;
    t) tsample="$OPTARG"
      ;;
    n) nsample="$OPTARG"
      ;;
    v) snp_vcf="$OPTARG"
      ;;
    p) threads="$OPTARG"
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

snp_pileup=${tsample}_vs_${nsample}.csv.gz
nbhd_snp=${tsample}_vs_${nsample}.tsv
gs_snp_pileup=${gs_snp_bucket}/results/calling/somatic_snp_pileup/${snp_pileup}
vm_snp_pileup=${vm_res_folder}/calling/somatic_snp_pileup/${snp_pileup}
gs_nbhd_snp=${gs_snp_bucket}/results/calling/somatic_nbhd_snp/${nbhd_snp}
vm_nbhd_snp=${vm_res_folder}/calling/somatic_nbhd_snp/${nbhd_snp}
gsutil ls ${gs_snp_pileup}
status=$?

mkdir -p ${vm_res_folder}/calling/somatic_snp_pileup
mkdir -p ${vm_res_folder}/calling/somatic_nbhd_snp

if [[ $status != 0 ]]; then
  printf -- "-INFO: snp-pileup file %s not found, create it from the BAM files\n" "${snp_pileup}"

  gs_tbam_file=${gs_bam_bucket}/${tsample}.bam
  gs_tbai_file=${gs_bam_bucket}/${tsample}.bai
  gs_nbam_file=${gs_bam_bucket}/${nsample}.bam
  gs_nbai_file=${gs_bam_bucket}/${nsample}.bai

  vm_tbam_file=${vm_res_folder}/mapping/${tsample}.bam
  vm_tbai_file=${vm_res_folder}/mapping/${tsample}.bai
  vm_nbam_file=${vm_res_folder}/mapping/${nsample}.bam
  vm_nbai_file=${vm_res_folder}/mapping/${nsample}.bai

  if [[ ! -f "${vm_tbam_file}" ]]; then
    printf "  copying file %s to the VM...\n" "${gs_tbam_file}"
    gsutil cp ${gs_tbam_file} ${vm_tbam_file}
  else
    printf "  BAM file %s already exists!\n" "${vm_tbam_file}"
  fi

  if [[ ! -f "${vm_tbai_file}" ]]; then
    printf "  copying file %s to the VM...\n" "${gs_tbai_file}"
    gsutil cp ${gs_tbai_file} ${vm_tbai_file}
  else
    printf "  BAM index file %s already exists!\n" "${vm_tbai_file}"
  fi

  if [[ ! -f "${vm_nbam_file}" ]]; then
    printf "  copying file %s to the VM...\n" "${gs_nbam_file}"
    gsutil cp ${gs_nbam_file} ${vm_nbam_file}
  else
    printf "  BAM file %s already exists!\n" "${vm_nbam_file}"
  fi

  if [[ ! -f "${vm_nbai_file}" ]]; then
    printf "  copying file %s to the VM...\n" "${gs_nbai_file}"
    gsutil cp ${gs_nbai_file} ${vm_nbai_file}
  else
    printf "  BAM index file %s already exists!\n" "${vm_nbai_file}"
  fi

  printf -- "-INFO: running Rscript 01.2_get_snp_pileup.R...\n"
  printf -- " path to Rscript command %s\n" "$(which Rscript)"

  Rscript workflow/scripts/01.2_get_snp_pileup.R \
    -t ${vm_tbam_file} \
    -n ${vm_nbam_file} \
    -op ${vm_snp_pileup} \
    -on ${vm_nbhd_snp} \
    -vcf ${snp_vcf} \
    -N ${threads}

  printf -- "-INFO: saving %s to the google bucket...\n" "${vm_snp_pileup}"
  gsutil cp ${vm_snp_pileup} ${gs_snp_pileup}

  printf -- "-INFO: saving %s to the google bucket...\n" "${vm_nbhd_snp}"
  gsutil cp ${vm_nbhd_snp} ${gs_nbhd_snp}
else
  printf "-INFO: snp-pileup file %s found, copy it to the VM\n" "${snp_pileup}"
  gsutil cp ${gs_snp_pileup} ${vm_snp_pileup}

  printf "-INFO: nbhd-snp file %s found, copy it to the VM\n" "${nbhd_snp}"
  gsutil cp ${gs_nbhd_snp} ${vm_nbhd_snp}
fi
