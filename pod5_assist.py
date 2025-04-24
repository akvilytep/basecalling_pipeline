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

# Step 1: do 10% mini sum
    
#run_command('for seqsum in */sequencing_summary_????????_????????_????????.txt; do filename=$(basename "${seqsum%.*}"); csvtk -t sample -p 0.01 "$seqsum" > "${filename}_1%_ss.txt"; done')

# Step 2: read_ids
read_ids = []
for filename in glob.glob("*/sequencing_summary_????????_????????_????????_1%_ss.txt"):
    print(f"Reading from: {filename}")
    df = pd.read_csv(filename, sep="\t")
    if "read_id" in df.columns:
        read_ids.extend(df["read_id"].tolist())
    else:
        raise ValueError(f"'read_id' column not found in {filename}")
    
# Convert all read_ids to strings
read_ids = list(map(str, read_ids))

# read_ids to a file
read_id_file = "read_id_list.txt"
with open(read_id_file, "w") as f:
    f.write("\n".join(read_ids))
print(f"Read IDs written to {read_id_file}")

# Step 3: Make output directory
os.makedirs("all_pod5", exist_ok=True)

# Step 4: Link all .pod5 files
run_command("find 2025*/pod5* -name '*.pod5' -exec ln -s $(realpath {}) all_pod5/ \;")






# Step 5: Filter .pod5 files to create a subset with only the extracted read IDs

# Create a directory for filtered .pod5 files
filtered_pod5_dir = "filtered_pod5"
os.makedirs(filtered_pod5_dir, exist_ok=True)

# Step 6: Combine filtered .pod5 files into a single output file

# Define the output file for the combined filtered .pod5
filtered_pod5_output = "1%_filtered"

# Run the pod5 filter command with --force-overwrite
run_command(
    f"pod5 filter all_pod5/*.pod5 --output {filtered_pod5_dir}/{filtered_pod5_output}.pod5 --ids {read_id_file} --force-overwrite --missing-ok"
)

print(f"Filtered .pod5 file created: {filtered_pod5_dir}/{filtered_pod5_output}.pod5")

# Step 7: Split the filtered .pod5 file into individual files by filename_pod5

# Find sequencing summary files
files = glob.glob("*/sequencing_summary_????????_????????_????????_1%_ss.txt")
print(f"Found files: {files}")
if not files:
    raise FileNotFoundError("No sequencing summary files found matching the pattern.")

# Read the sequencing summary file(s)
dfs = []
for file in files:
    print(f"Reading sequencing summary file: {file}")
    dfs.append(pd.read_csv(file, sep="\t"))

# Concatenate all DataFrames into one
df = pd.concat(dfs, ignore_index=True)

# Extract the grouping key from the filename_pod5 column
df["group_key"] = df["filename_pod5"].str.extract(r"^(.*?)_")[0]

# Group by the extracted group_key
for group_key, group in df.groupby("group_key"):
    output_pod5_file = f"{filtered_pod5_dir}/{group_key}.pod5"  # Define the output filename

    # Create a temporary read_id file for this group
    group_read_ids = group["read_id"].tolist()
    group_read_id_file = f"{filtered_pod5_dir}/read_ids_{group_key}.txt"
    with open(group_read_id_file, "w") as f:
        f.write("\n".join(map(str, group_read_ids)))

    # Filter the combined filtered .pod5 file for this group
    run_command(
        f"pod5 filter {filtered_pod5_dir}/{filtered_pod5_output}.pod5 --output {output_pod5_file} --ids {group_read_id_file} --force-overwrite --missing-ok"
    )
    print(f"Created individual pod5 file: {output_pod5_file}")

    # Clean up the temporary read_id file
    os.remove(group_read_id_file)
