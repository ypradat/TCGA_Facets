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
printf -- "-INFO: this script will check for instances that have terminated and restart them every %s seconds\n" "${frequency}"

# identify instances that are alive (running or terminated)
IFS=$'\n' read -r -d '' -a instances_alive < <( gcloud compute instances list --zones=us-central1-a --filter="name~facets-tcga-[\d]+" --format="value(NAME)" 2> >(grep -v "WARNING") && printf '\0' )

i=1
while [[ ${#instances_alive[@]} != 0 ]]
do
    printf -- "-CHECK %s\n" "${i}"
    IFS=$'\n' read -r -d '' -a instances_alive < <( gcloud compute instances list --zones=us-central1-a --filter="name~facets-tcga-[\d]+" --format="value(NAME)" 2> >(grep -v "WARNING") && printf '\0' )
    IFS=$'\n' read -r -d '' -a instances_running < <( gcloud compute instances list --zones=us-central1-a --filter="name~facets-tcga-[\d]+ AND status=RUNNING" --format="value(NAME)" 2> >(grep -v "WARNING") && printf '\0' )
    IFS=$'\n' read -r -d '' -a instances_terminated < <( gcloud compute instances list --zones=us-central1-a --filter="name~facets-tcga-[\d]+ AND status=TERMINATED" --format="value(NAME)" 2> >(grep -v "WARNING") && printf '\0' )

    printf -- "  %s instance(s) ALIVE\n" "${#instances_alive[@]}"
    printf -- "  %s RUNNING/%s TERMINATED\n" "${#instances_running[@]}" "${#instances_terminated[@]}"
    
    if (( ${#instances_terminated[@]} != 0 ))
    then
	printf -- "  restarting TERMINATED instance(s)...\n"
	for instance_terminated in "${instances_terminated[@]}"
	do
	    gcloud compute instances start ${instance_terminated}  \
                --project=isb-cgc-external-001 \
                --zone=us-central1-a
	done
    fi

    sleep ${frequency}s
    ((i++))
done
