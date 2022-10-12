#!/bin/bash

while getopts ":b:" opt; do
    case $opt in
	b) batch_index="$OPTARG"
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

# Populate the bucket tcga_wxs_bam
python -u gcloud/populate_bam_gs_bucket.py \
   --samples_table "config/samples.all.tsv" \
   --bucket_gs_uri "gs://tcga_wxs_bam" \
   --batch_index ${batch_index}

# Extract disk size required for instance, considering a 20gb margin on top of the
# BAM file sizes.
file_sizes=$(awk -F '\t' \
    '{if (NR==1) {sum=0} else if ($(NF)==1) {sum += $(NF-1)}} END {print sum;}' \
    config/tumor_normal_pairs.all.tsv)
instance_size=$(echo $file_sizes| awk '{print int($1+20)}')

# Create the instance and run the pipeline via the startup script
gcloud compute instances create facets-tcga-${batch_index} \
    --project=isb-cgc-external-001 \
    --zone=us-central1-a \
    --machine-type=e2-highmem-8 \
    --network-interface=network-tier=PREMIUM,subnet=default \
    --maintenance-policy=MIGRATE \
    --provisioning-model=STANDARD \
    --service-account=482716779852-compute@developer.gserviceaccount.com \
    --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
    --enable-display-device \
    --tags=https-server \
    --create-disk=auto-delete=yes,boot=yes,device-name=facets-tcga-64,image=projects/debian-cloud/global/images/debian-11-bullseye-v20220920,mode=rw,size=${instance_size},type=projects/isb-cgc-external-001/zones/us-central1-a/diskTypes/pd-balanced \
    --no-shielded-secure-boot \
    --shielded-vtpm \
    --shielded-integrity-monitoring \
    --reservation-affinity=any \
    --metadata-from-file=startup-script=./gcloud/startup_gcloud_vm.sh \
    --metadata=batch_index=${batch_index}

# # Delete instance
# gcloud compute instances delete facets-tcga-${batch_index} \
#     --zone=us-central1-a \
#     --delete-disks=all \
#     --quiet
 
# Clean the bucket tcga_wxs_bam
python -u gcloud/depopulate_bam_gs_bucket.py \
   --samples_table config/samples.all.tsv \
   --bucket_gs_uri "gs://tcga_wxs_bam" \
   --batch_index ${batch_index}
