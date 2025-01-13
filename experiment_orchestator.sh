#!/usr/bin/env bash

# Possible parameters
PHYSICAL_SCENARIOS=(0 1)  # 0 for baseline physical, 1 for impaired physical
DELAYS=("0.5ms" "1ms" "5ms" "10ms")
ERROR_RATES=("0.0001" "0.01")

DATE_STR=$(date +"%Y%m%d_%H%M%S")

mkdir -p results

for phys in "${PHYSICAL_SCENARIOS[@]}"; do
    for delay in "${DELAYS[@]}"; do
        # If delay == 0.5ms, we use baseline error rate
        # else we use impaired error rate
        if [ "$delay" = "0.5ms" ]; then
            err="0.0001" # baseline cyber
        else
            err="0.01"   # impaired cyber
        fi

        # Run the simulation
        echo "Running simulation with enable_events=$phys, link_delay=$delay, link_error_rate=$err"
        python simulator_demo_RIDE.py \
            --enable_events=$phys \
            --link_delay=$delay \
            --link_error_rate=$err

        # Once completed, rename/move the output file
        # Collector produces CollectorStore.hd5 file:
        SCENARIO_NAME="phys${phys}_delay${delay}_err${err}_${DATE_STR}"
        mv CollectorStore.hd5 results/${SCENARIO_NAME}_CollectorStore.hd5
    done
done
