#!/bin/bash

set -Eeuo pipefail

base_config="base_configs/base_1.yaml"

grid_config_files=(
    # LSM
    "grid_configs/lsm/enable_bloom_filter_1.yaml"
    "grid_configs/lsm/max_bytes_for_level_multiplier_1.yaml"
    "grid_configs/lsm/max_l0_files.yaml"
    "grid_configs/lsm/write_buffer_size_1.yaml"
    # Disk
    "grid_configs/disk/type_1.yaml"
    # Compaction
    "grid_configs/compaction/type_1.yaml"
    # Workload
    "grid_configs/workload/type_1.yaml"
)

# Generate timestamp once to ensure consistent filenames
timestamp=$(date +"%Y_%m_%d__%H_%M_%S")

for grid_config in "${grid_config_files[@]}"; do
    if [[ ! -f "$grid_config" ]]; then
        echo "Warning: Grid config file '$grid_config' not found, skipping."
        continue
    fi

    echo "Base config: $base_config, Grid config: $grid_config"

    output_dir="results/$timestamp/$(basename "$base_config")__$(basename "$grid_config")"

    python run.py -b "$base_config" -g "$grid_config" -o "$output_dir"
done
