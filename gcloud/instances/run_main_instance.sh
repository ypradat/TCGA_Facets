#!/bin/bash

usage() { echo "$0 Usage:" && grep " .)\ #" $0; exit 0; }

while getopts ":a:b h" opt; do
    case $opt in
        a) # Minimum main index to be run.
            main_min="$OPTARG"
            ;;
        b) # Maximum main index to be run.
            main_max="$OPTARG"
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

main_indices=($(seq ${main_min} 1 ${main_max}))

for main_index in "${main_indices[@]}"
do
    # Create the instance and run the pipeline via the startup script
    gcloud compute instances create facets-tcga-main-${main_index} \
	--project=isb-cgc-external-001 \
	--zone=us-central1-a \
	--machine-type=e2-small \
	--network-interface=network-tier=PREMIUM,subnet=default \
	--no-restart-on-failure \
	--maintenance-policy=MIGRATE \
	--provisioning-model=STANDARD \
	--service-account=482716779852-compute@developer.gserviceaccount.com \
	--scopes=https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.read_write,https://www.googleapis.com/auth/logging.admin,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management,https://www.googleapis.com/auth/trace.append \
	--enable-display-device \
	--tags=https-server \
	--create-disk=auto-delete=yes,boot=yes,device-name=facets-tcga-main,image=projects/debian-cloud/global/images/debian-11-bullseye-v20220920,mode=rw,size=10,type=projects/isb-cgc-external-001/zones/us-central1-a/diskTypes/pd-balanced \
	--no-shielded-secure-boot \
	--shielded-vtpm \
	--shielded-integrity-monitoring \
	--reservation-affinity=any \
	--metadata-from-file=startup-script=./gcloud/instances/startup_main_instance.sh
done
