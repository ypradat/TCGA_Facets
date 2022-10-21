#!/bin/bash

dry_run="no"
skip_bam_copy="no"

while getopts ":a:b:i::ns" opt; do
    case $opt in
	a) batch_min="$OPTARG"
	    ;;
	b) batch_max="$OPTARG"
	    ;;
	i) batch_idx="$OPTARG"
	    ;;
	n) dry_run="yes"
	    ;;
	s) skip_bam_copy="yes"
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

if [[ -z "${batch_idx}" ]]; then
    # generate an array of batch indices between batch_min and batch_max and remove from this list
    # indices already processed or indices that failed twice.
    batch_list=gcloud/batch_indices.txt
    python -u gcloud/others/generate_batch_indices.py \
        --samples_table config/samples.all.tsv \
        --logs_uri "gs://facets_tcga_results/logs/gcloud" \
        --logs_failed_uri "gs://facets_tcga_results/logs/gcloud_failed" \
        --batch_min ${batch_min} \
        --batch_max ${batch_max} \
        --batch_list ${batch_list}

    # read the list of batch indices to be processed.
    batch_indices=()
    while IFS= read -r line; do
       batch_indices+=("$line")
    done <${batch_list}
    rm ${batch_list}
else
    # generate a length-1 array of indices
    batch_indices=("${batch_idx}")
fi

if [[ ${dry_run} == "yes" ]]; then
    printf "this is a dry-run.\n"
    for batch_index in "${batch_indices[@]}"
    do
	printf "running batch: %s ...\n" "${batch_index}"
    done
else
    for batch_index in "${batch_indices[@]}"
    do
	printf "\n======================================\n"
	printf "running batch: %s ...\n" "${batch_index}"

	# Add BAMs to the bucket
	if [[ ${skip_bam_copy} == "yes" ]]; then
	    printf "skipping copy of BAM files from GDC-controlled bucket\n"
	else
	    printf "copying BAM files from GDC-controlled bucket...\n"
	    python -u gcloud/buckets/populate_bam_gs_bucket.py \
	       --samples_table config/samples.all.tsv \
	       --bucket_gs_uri "gs://tcga_wxs_bam" \
	       --batch_index ${batch_index}
	    printf "\n\n"
	fi

	# Extract disk size required for instance, considering a 50gb margin on top of the
	# BAM file sizes.
	file_sizes=$(awk -F '\t' -v i="${batch_index}" \
	    '{if (NR==1) {sum=0} else if ($(NF)==i) {sum += $(NF-1)}} END {print sum;}' \
	    config/samples.all.tsv)
	instance_size=$(echo $file_sizes| awk '{print int($1+50)}')

	# Define VM RAM size according to previous failed logs
	# If the pipeline has already failed three times for this batch due to memory usage, make one last try
	# with a 128-Gb RAM VM. For all runs, use the 64-Gb RAM VM.
	gsutil ls gs://facets_tcga_results/logs/gcloud_failed/startup_gcloud_vm_third_oom_${batch_index}.log &> /dev/null
	status_failed_third_oom=$?

	if [[ ${status_failed_third_oom} == 0 ]]; then
	    machine_type="e2-highmem-16"
	else
	    machine_type="e2-highmem-8"
	fi

	# Create the instance and run the pipeline via the startup script
	gcloud compute instances create facets-tcga-${batch_index} \
	    --project=isb-cgc-external-001 \
	    --zone=us-central1-a \
	    --machine-type=${machine_type} \
	    --network-interface=network-tier=PREMIUM,subnet=default \
	    --no-restart-on-failure \
	    --maintenance-policy=TERMINATE \
	    --provisioning-model=STANDARD \
	    --service-account=482716779852-compute@developer.gserviceaccount.com \
	    --scopes=https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.read_write,https://www.googleapis.com/auth/logging.admin,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management,https://www.googleapis.com/auth/trace.append \
	    --enable-display-device \
	    --tags=https-server \
	    --create-disk=auto-delete=yes,boot=yes,device-name=facets-tcga-${instance_size},image=projects/debian-cloud/global/images/debian-11-bullseye-v20220920,mode=rw,size=${instance_size},type=projects/isb-cgc-external-001/zones/us-central1-a/diskTypes/pd-balanced \
	    --no-shielded-secure-boot \
	    --shielded-vtpm \
	    --shielded-integrity-monitoring \
	    --reservation-affinity=any \
	    --provisioning-model=SPOT \
	    --preemptible \
	    --metadata-from-file=startup-script=./gcloud/instances/startup_instance.sh,shutdown-script=./gcloud/instances/shutdown_instance.sh \
	    --metadata=batch_index=${batch_index}
    done
fi
