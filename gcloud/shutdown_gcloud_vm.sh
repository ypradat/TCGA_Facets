#!/bin/bash

batch_index=$(curl http://metadata.google.internal/computeMetadata/v1/instance/attributes/batch_index -H "Metadata-Flavor: Google")
local_log_name=startup_gcloud_vm_${batch_index}.log
preempted=/home/ypradat/preempted.done

exec 3>&1 4>&2 >>/home/ypradat/${local_log_name} 2>&1

now_date="$(date +'%d/%m/%Y')"
now_time="$(date +'%T')"
printf "\nStart date and time at preemption: %s %s\n" "$now_date" "$now_time"
touch $preempted
