#!/bin/bash

while getopts ":f:" opt; do
    case $opt in
	f) frequency="$OPTARG"
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

# message
printf -- "-INFO: this script will check for instances that have been terminated/deleted on fail and restart/recreate them every %s seconds\n" "${frequency}"

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
        --prefix "startup_gcloud_vm_first_" 2> >(grep -v "WARNING") && printf '\0' )

    IFS=$'\n' read -r -d '' -a indices_deleted_second < <( python -u gcloud/others/print_batch_indices.py \
        --logs_uri "gs://facets_tcga_results/logs/gcloud_failed" \
        --prefix "startup_gcloud_vm_second_" 2> >(grep -v "WARNING") && printf '\0' )

    IFS=$'\n' read -r -d '' -a indices_deleted_third < <( python -u gcloud/others/print_batch_indices.py \
        --logs_uri "gs://facets_tcga_results/logs/gcloud_failed" \
        --prefix "startup_gcloud_vm_third_" 2> >(grep -v "WARNING") && printf '\0' )

    printf -- "  %s instance(s) ALIVE/%s instance(s) have failed\n" \
	"${#instances_alive[@]}" \
	"$(( ${#indices_deleted_first[@]} + ${#indices_deleted_second[@]} + ${#indices_deleted_third[@]} ))"

    printf -- "  %s RUNNING/%s TERMINATED/%s FAILED 1st run/%s FAILED 2nd run/%s FAILED 3rd run\n" \
	"${#instances_running[@]}" \
	"${#instances_terminated[@]}" \
       	"${#indices_deleted_first[@]}" \
       	"${#indices_deleted_second[@]}" \
       	"${#indices_deleted_third[@]}" 
    
    if (( ${#instances_terminated[@]} != 0 ))
    then
        printf -- "  restarting %s TERMINATED instances\n" \
            "${#instances_terminated[@]}"
        for instance_terminated in "${instances_terminated[@]}"
        do
            gcloud compute instances start ${instance_terminated}  \
                --project=isb-cgc-external-001 \
                --zone=us-central1-a
        done
    fi

    if (( ${#indices_deleted_first[@]} != 0 ))
    then
        printf -- "  recreating %s FAILED 1st run instances\n" \
            "${#indices_deleted_first[@]}"
        for index_deleted_first in "${indices_deleted_first[@]}"
        do
            bash gcloud/instances/run_instances.sh -i ${index_deleted_first} -s
        done
    fi

    if (( ${#indices_deleted_second[@]} != 0 ))
    then
        printf -- "  recreating %s FAILED 2nd run instances\n" \
            "${#indices_deleted_second[@]}"
        for index_deleted_second in "${indices_deleted_second[@]}"
        do
            bash gcloud/instances/run_instances.sh -i ${index_deleted_second} -s
        done
    fi

    sleep ${frequency}s
    ((i++))
done
