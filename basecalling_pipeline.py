#!/usr/bin/env python3

import subprocess
import os
import pandas as pd
import glob

def run_command(cmd, shell=True):
    print(f"\n>>> Running: {cmd}")
    result = subprocess.run(cmd, shell=shell)
    if result.returncode != 0:
        raise RuntimeError(f"\n!!! Command failed: {cmd}")

# Step 1: Prompt for kit (moved to the beginning)
kit = input("\nEnter kit used (LSK114, RBK24, RBK96, NBD24, NBD96, RNA, ULK114): ").strip().upper()

# Prompt for basecalling option
basecalling_option = input("\nEnter basecalling option (HAC or SUP): ").strip().upper()
if basecalling_option not in ["HAC", "SUP"]:
    raise ValueError("Invalid basecalling option. Choose either 'HAC' or 'SUP'.")

# Define basecalling parameters per kit
kit_settings = {
    "LSK114": {
        "barcode_flag": "",
        "ref": "/data/refs/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.mmi",
        "config": f"/opt/ont/dorado/data/dna_r10.4.1_e8.2_400bps_5khz_{basecalling_option.lower()}.cfg"
    },
    "RBK24": {
        "barcode_flag": "--barcode_kits 'SQK-RBK114-24'",
        "ref": "/data/refs/lambda_757a991a.fasta",
        "config": f"/opt/ont/dorado/data/dna_r10.4.1_e8.2_400bps_5khz_{basecalling_option.lower()}.cfg"
    },
    "RBK96": {
        "barcode_flag": "--barcode_kits 'SQK-RBK114-96'",
        "ref": "/data/refs/lambda_757a991a.fasta",
        "config": f"/opt/ont/dorado/data/dna_r10.4.1_e8.2_400bps_5khz_{basecalling_option.lower()}.cfg"
    },
    "NBD24": {
        "barcode_flag": "--barcode_kits 'SQK-NBD114-24'",
        "ref": "/data/refs/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.mmi",
        "config": f"/opt/ont/dorado/data/dna_r10.4.1_e8.2_400bps_5khz_{basecalling_option.lower()}.cfg"
    },
    "NBD96": {
        "barcode_flag": "--barcode_kits 'SQK-NBD114-96'",
        "ref": "/data/refs/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.mmi",
        "config": f"/opt/ont/dorado/data/dna_r10.4.1_e8.2_400bps_5khz_{basecalling_option.lower()}.cfg"
    },
    "RNA": {
        "barcode_flag": "",  
        "ref": "/data/refs/RNA_YHR174W.fasta",
        "config": f"/opt/ont/dorado/data/rna_rp4_130bps_{basecalling_option.lower()}.cfg"
    },
    "ULK114": {
        "barcode_flag": "",
        "ref": "/data/refs/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.mmi",
        "config": f"/opt/ont/dorado/data/dna_r10.4.1_e8.2_400bps_5khz_{basecalling_option.lower()}.cfg"
    }
}

# Validate kit
if kit not in kit_settings:
    raise ValueError(f"Invalid kit: {kit}. Choose from {', '.join(kit_settings.keys())}")

# Step 2: Extract read IDs (updated with fallback mechanism)

read_ids = []
if kit == "ULK114":
    print("Looking for 10% summaries...")
    for filename in glob.glob("*/sequencing_summary_????????_????????_????????_10%_ss.txt"):
        print(f"Reading from: {filename}")
        df = pd.read_csv(filename, sep="\t")
        if "read_id" in df.columns:
            read_ids.extend(df["read_id"].tolist())
        else:
            raise ValueError(f"'read_id' column not found in {filename}")
else:
    print("Looking for 1% summaries...")
    for filename in glob.glob("*/sequencing_summary_????????_????????_????????_1%_ss.txt"):
        print(f"Reading from: {filename}")
        df = pd.read_csv(filename, sep="\t")
        if "read_id" in df.columns:
            read_ids.extend(df["read_id"].tolist())
        else:
            raise ValueError(f"'read_id' column not found in {filename}")

# Fallback: Generate summaries if no valid files are found
if not read_ids:
    print("No valid 1% or 10% sequencing summary files found. Generating summaries...")
    if kit == "ULK114":
        run_command(
            'for seqsum in */sequencing_summary_????????_????????_????????.txt; '
            'do filename=$(basename "${seqsum%.*}"); '
            'csvtk -t sample -p 0.1 "$seqsum" > "${filename}_10%_ss.txt"; '
            'done'
        )
    else:
        run_command(
            'for seqsum in */sequencing_summary_????????_????????_????????.txt; '
            'do filename=$(basename "${seqsum%.*}"); '
            'csvtk -t sample -p 0.01 "$seqsum" > "${filename}_1%_ss.txt"; '
            'done'
        )
    # Retry reading summaries after generating them
    summary_pattern = "*/sequencing_summary_????????_????????_????????_10%_ss.txt" if kit == "ULK114" else "*/sequencing_summary_????????_????????_????????_1%_ss.txt"
    for filename in glob.glob(summary_pattern):
        print(f"Reading from: {filename}")
        df = pd.read_csv(filename, sep="\t")
        if "read_id" in df.columns:
            read_ids.extend(df["read_id"].tolist())
        else:
            raise ValueError(f"'read_id' column not found in {filename}")

if not read_ids:
    raise FileNotFoundError("Failed to generate or find valid sequencing summary files.")

# Step 3: Make output directory
os.makedirs("all_pod5", exist_ok=True)

# Step 4: Link all .pod5 files
run_command("find 2025*/pod5* -name '*.pod5' -exec ln -s $(realpath {}) all_pod5/ \;")

# Build basecall command
barcode_flag = kit_settings[kit]["barcode_flag"]
ref_path = kit_settings[kit]["ref"]
config_path = kit_settings[kit]["config"]

basecall_cmd = (
    f"ont_basecall_client --port /tmp/.guppy/5555 -r -i all_pod5/ -s output/ "
    f"-c {config_path} "
    f"-l read_id_list_all.txt "
    f"{barcode_flag} "
    f"-a {ref_path} "
    f"--server_file_load_timeout 600"
)

print(f"\n>>> Running basecalling for kit: {kit}")
run_command(basecall_cmd)

# Final: Process sequencing summary
df = pd.read_csv("output/sequencing_summary.txt", sep="\t")
df["flowcell_id"] = df["filename"].str[:8]

for (flowcell_id, run_id), group in df.groupby(["flowcell_id", "run_id"]):
    run_id_short = run_id[:8]
    output_filename = f"sequencing_summary_{flowcell_id}_{run_id_short}_{basecalling_option}_1%.txt"
    print(f"Writing: {output_filename} with {len(group)} rows")
    group.to_csv(output_filename, sep="\t", index=False)

