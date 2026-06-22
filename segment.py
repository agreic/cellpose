#!/usr/bin/env python
"""
Simple segmentation script that segments images using cellpose.

Usage:
    uv run segment.py
"""

import subprocess
import sys

# Cellpose configuration
cellpose_config = {
    "input_dir": "T:/260402SA11/extracted_t00031_png/w00_only",
    "output_dir": "T:/260402SA11/Analysis/Segmentation_260504",
    "diameter": 30.0,
    "flow_threshold": 3,
    "cellprob_threshold": 1.2,
    "batch_size": 32,
    "norm_percentile_low": 1.0,
    "norm_percentile_high": 99.0,
}

print(f"Input directory: {cellpose_config['input_dir']}")
print(f"Output directory: {cellpose_config['output_dir']}")
print("\n" + "="*60)

# Build cellpose command
cellpose_command = [
    "uv", "run", "cellpose",
    "--dir", cellpose_config["input_dir"],
    "--savedir", cellpose_config["output_dir"],
    "--diameter", str(cellpose_config["diameter"]),
    "--flow_threshold", str(cellpose_config["flow_threshold"]),
    "--cellprob_threshold", str(cellpose_config["cellprob_threshold"]),
    "--batch_size", str(cellpose_config["batch_size"]),
    "--norm_percentile", str(cellpose_config["norm_percentile_low"]), str(cellpose_config["norm_percentile_high"]),
    "--use_gpu",
    "--save_png",
    "--no_npy",
    "--verbose",
]

print(f"Running: {' '.join(cellpose_command)}\n")

# Run cellpose
try:
    result = subprocess.run(cellpose_command, check=True)
    print("="*60)
    print("\nSegmentation completed successfully!")
    sys.exit(0)
except subprocess.CalledProcessError as e:
    print("="*60)
    print(f"\nError during segmentation: {e}")
    sys.exit(1)
