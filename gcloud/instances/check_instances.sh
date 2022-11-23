#!/bin/bash

usage() { echo "$0 Usage:" && grep " .)\ #" $0; exit 0; }

while getopts ":m:f:t:u: h" opt; do
  case $opt in
    m) # time in seconds separating two consecutive checks.
      time="$OPTARG"
      ;;
    f) # <download_bam|get_snp_pileup|somatic_cnv_facets|somatic_cnv_process_vcf> Rule name from which the pipeline is started. 
      start_from="$OPTARG"
      [[ $start_from =~ ^(download_bam|get_snp_pileup|somatic_cnv_facets|somatic_cnv_process_vcf)$ ]] || usage
      ;;
    t) # Github token for downloading the pipeline code.
      github_token="$OPTARG"
      ;;
    u) # A given pair will be ingored only if expected ouptut files already exist and were updated after this date. DD/MM/YYYY format.
      update_date_min="$OPTARG"
      ;;
    h) # Display help.
      usage
      ;;
    *) echo "Invalid option -$OPTARG" >&2
      usage
      exit 1
      ;;
  esac
done

# message
printf -- "-INFO: this script will check for instances that have been terminated/deleted on fail and restart/recreate them every %s seconds\n" "${time}"

i=1
while [[ $i == 1 ]] || [[ ${#instances_alive[@]} != 0 ]] || [[ ${#indices_deleted_first[@]} != 0 ]] || [[ ${#indices_deleted_second[@]} != 0 ]]
do
  printf -- "-CHECK %s\n" "${i}"

  # use gcloud compute instances list command to identify instances alive/running/terminated
  IFS=$'\n' read -r -d '' -a instances_alive < <( gcloud compute instances list \
    --zones=us-central1-a \
    --filter="name~facets-tcga-[\d]+" \
    --format="value(NAME)" 2> >(grep -v "WARNING") && printf '\0' )

  IFS=$'\n' read -r -d '' -a instances_running < <( gcloud compute instances list \
    --zones=us-central1-a \
    --filter="name~facets-tcga-[\d]+ AND status=RUNNING" \
    --format="value(NAME)" 2> >(grep -v "WARNING") && printf '\0' )

  IFS=$'\n' read -r -d '' -a instances_terminated < <( gcloud compute instances list \
    --zones=us-central1-a \
    --filter="name~facets-tcga-[\d]+ AND status=TERMINATED" \
    --format="value(NAME)" 2> >(grep -v "WARNING") && printf '\0' )

  # use logs in the folder gs://facets_tcga_results/logs/gcloud_failed to identify instances that have failed
  IFS=$'\n' read -r -d '' -a indices_deleted_first < <( python -u gcloud/others/print_batch_indices.py \
    --logs_uri "gs://facets_tcga_results/logs/gcloud_failed" \
    --prefix "startup_gcloud_vm_first_" \
    --ignore_terminated \
    --ignore_running 2> >(grep -v "WARNING") && printf '\0' )

  IFS=$'\n' read -r -d '' -a indices_deleted_second < <( python -u gcloud/others/print_batch_indices.py \
    --logs_uri "gs://facets_tcga_results/logs/gcloud_failed" \
    --prefix "startup_gcloud_vm_second_" \
    --ignore_terminated \
    --ignore_running 2> >(grep -v "WARNING") && printf '\0' )

  IFS=$'\n' read -r -d '' -a indices_deleted_third_oom < <( python -u gcloud/others/print_batch_indices.py \
    --logs_uri "gs://facets_tcga_results/logs/gcloud_failed" \
    --prefix "startup_gcloud_vm_third_oom_" \
    --ignore_terminated \
    --ignore_running 2> >(grep -v "WARNING") && printf '\0' )

  IFS=$'\n' read -r -d '' -a indices_deleted_third_oth < <( python -u gcloud/others/print_batch_indices.py \
    --logs_uri "gs://facets_tcga_results/logs/gcloud_failed" \
    --prefix "startup_gcloud_vm_third_oth_" \
    --ignore_terminated \
    --ignore_running 2> >(grep -v "WARNING") && printf '\0' )

  # use logs in the folder gs://facets_tcga_results/logs/gcloud_rerun to identify instances that need rerunning
  IFS=$'\n' read -r -d '' -a indices_rerun_first < <( python -u gcloud/others/print_batch_indices.py \
    --logs_uri "gs://facets_tcga_results/logs/gcloud_rerun" \
    --prefix "startup_gcloud_vm_first_" \
    --ignore_terminated \
    --ignore_running 2> >(grep -v "WARNING") && printf '\0' )

  IFS=$'\n' read -r -d '' -a indices_rerun_second < <( python -u gcloud/others/print_batch_indices.py \
    --logs_uri "gs://facets_tcga_results/logs/gcloud_rerun" \
    --prefix "startup_gcloud_vm_second_" \
    --ignore_terminated \
    --ignore_running 2> >(grep -v "WARNING") && printf '\0' )

  printf -- " %s instance(s) ALIVE/%s instance(s) need rerunning/%s instance(s) have failed\n" \
    "${#instances_alive[@]}" \
    "$(( ${#indices_rerun_first[@]} + ${#indices_rerun_second[@]} ))" \
    "$(( ${#indices_deleted_first[@]} + ${#indices_deleted_second[@]} + ${#indices_deleted_third_oom[@]} + ${#indices_deleted_third_oth[@]} ))"

  printf -- " %s RUNNING/%s TERMINATED/%s RERUN 1st run/%s RERUN 2nd run/%s FAILED 1st run/%s FAILED 2nd run/%s FAILED 3rd run oom/%s FAILED 3rd run other\n" \
    "${#instances_running[@]}" \
    "${#instances_terminated[@]}" \
    "${#indices_rerun_first[@]}" \
    "${#indices_rerun_second[@]}" \
    "${#indices_deleted_first[@]}" \
    "${#indices_deleted_second[@]}" \
    "${#indices_deleted_third_oom[@]}" \
    "${#indices_deleted_third_oth[@]}"
  
  if (( ${#instances_terminated[@]} != 0 ))
  then
    printf -- " restarting %s TERMINATED instances\n" \
      "${#instances_terminated[@]}"
    for instance_terminated in "${instances_terminated[@]}"
    do
      gcloud compute instances start ${instance_terminated} \
        --project=isb-cgc-external-001 \
        --zone=us-central1-a
    done
  fi


  if (( ${#indices_rerun_first[@]} != 0 ))
  then
    printf -- " recreating %s RERUN 1st run instances\n" \
      "${#indices_rerun_first[@]}"
    for index_rerun_first in "${indices_rerun_first[@]}"
    do
      bash gcloud/instances/run_instances.sh \
        -i ${index_rerun_first} \
        -s \
        -f ${start_from} \
        -t ${github_token} \
        -u ${update_date_min}
    done
  fi

  if (( ${#indices_rerun_second[@]} != 0 ))
  then
    printf -- " recreating %s RERUN 2nd run instances\n" \
      "${#indices_rerun_second[@]}"
    for index_rerun_second in "${indices_rerun_second[@]}"
    do
      bash gcloud/instances/run_instances.sh \
        -i ${index_rerun_second} \
        -s \
        -f ${start_from} \
        -t ${github_token} \
        -u ${update_date_min}
    done
  fi

  if (( ${#indices_deleted_first[@]} != 0 ))
  then
    printf -- " recreating %s FAILED 1st run instances\n" \
      "${#indices_deleted_first[@]}"
    for index_deleted_first in "${indices_deleted_first[@]}"
    do
      bash gcloud/instances/run_instances.sh \
        -i ${index_deleted_first} \
        -s \
        -f ${start_from} \
        -t ${github_token} \
        -u ${update_date_min}
    done
  fi

  if (( ${#indices_deleted_second[@]} != 0 ))
  then
    printf -- " recreating %s FAILED 2nd run instances\n" \
      "${#indices_deleted_second[@]}"
    for index_deleted_second in "${indices_deleted_second[@]}"
    do
      bash gcloud/instances/run_instances.sh \
        -i ${index_deleted_second} \
        -s \
        -f ${start_from} \
        -t ${github_token} \
        -u ${update_date_min}
    done
  fi

  sleep ${time}s
  ((i++))
done
