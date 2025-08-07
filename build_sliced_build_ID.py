#!/usr/bin/env python3
import os
import argparse
import pandas as pd
import math
from datetime import datetime
from jobgen import generate_jobs_from_excel

# ─── CONFIG ────────────────────────────────────────────────────────────────
# NOTE: These paths are relative to the project root.
INPUT_DIR = "Data Input"
OUTPUT_DIR = "schedules" # Or a more specific directory if preferred

MPP_FILE        = os.path.join(INPUT_DIR, "MPP Data 13 Easels.csv")
BOM_FILE        = os.path.join(INPUT_DIR, "Easel Components BOM.csv")
SLICED_FILE     = os.path.join(INPUT_DIR, "Sliced Build Generated.xlsx")
JOBS_OUTPUT_CSV = os.path.join(OUTPUT_DIR, "Jobs_Output.csv")
# ────────────────────────────────────────────────────────────────────────────

def build_sliced_build_ID(mpp_path: str, bom_path: str, out_path: str):
    # 1. Load and normalize source sheets
    df_mpp = pd.read_csv(mpp_path, parse_dates=['Overall_Due_Date'])
    df_mpp.columns = [c.strip().replace(' ', '_') for c in df_mpp.columns]
    df_bom = pd.read_csv(bom_path)
    # Normalize BOM column names
    df_bom.columns = [c.strip().replace(' ', '_') for c in df_bom.columns]
    # Normalize SKU formatting: convert dashes to underscores to match MPP
    df_bom['Product_SKU'] = df_bom['Product_SKU'].str.replace('-', '_')
    # If BOM lacks Product_SKU, but MPP has exactly one SKU, inject it
    if 'Product_SKU' not in df_bom.columns:
        unique_skus = df_mpp['Product_SKU'].unique()
        if len(unique_skus) == 1:
            df_bom['Product_SKU'] = unique_skus[0]
        else:
            raise KeyError("BOM missing 'Product_SKU' column and multiple SKUs found in MPP")
    df_bom.columns = [c.strip().replace(' ', '_') for c in df_bom.columns]

    # 2. Merge BOM template → MPP orders on Product_SKU
    df = df_bom.merge(
        df_mpp[['MPP_Item_ID','Product_SKU','Overall_Due_Date','Status','Project_Phase']],
        on='Product_SKU',
        how='inner'
    )

    # 3. Compute runs (vectorized): ceil(Print_Quantity / Alpha_Quantity_on_Plate)
    ratio = df['Print_Quantity'] / df['Alpha_Quantity_on_Plate']
    df['Number_of_Runs'] = ratio.apply(math.ceil).astype(int)

    # 4. Explode into slice-build rows
    rows = []
    for _, r in df.iterrows():
        for run in range(1, r['Number_of_Runs'] + 1):
            rows.append({
                'Sliced Build ID': f"{r['MPP_Item_ID']}–{r['Part_Name']}_R{run}",
                'Quantity of Runs': r['Number_of_Runs'],
                'Alpha Quantity on Plate': r['Alpha_Quantity_on_Plate'],
                'Estimated Print Time Minutes': int(pd.to_timedelta(r['Duration']).total_seconds() / 60),
                'Required Material ID': r.get('Required_Material_ID', ''),
                'Technology': r['Printing_Method'],
                'Status': 'Queued',
                'Created Timestamp': datetime.now().isoformat()
            })
# 5. Write to Excel for downstream consumption
    pd.DataFrame(rows).to_excel(out_path, index=False)
    print(f"✅ Sliced build sheet written to {out_path} ({len(rows)} rows)")


def main():
    parser = argparse.ArgumentParser(description="Generate a sliced build sheet from MPP and BOM data.")
    parser.add_argument('--mpp', type=str, default=MPP_FILE, help=f'Path to the MPP data CSV file. Default: {MPP_FILE}')
    parser.add_argument('--bom', type=str, default=BOM_FILE, help=f'Path to the BOM data CSV file. Default: {BOM_FILE}')
    parser.add_argument('--sliced-out', type=str, default=SLICED_FILE, help=f'Path for the output sliced build Excel file. Default: {SLICED_FILE}')
    parser.add_argument('--jobs-out', type=str, default=JOBS_OUTPUT_CSV, help=f'Path for the output jobs CSV file. Default: {JOBS_OUTPUT_CSV}')
    
    args = parser.parse_args()

    # --- Pre-flight Checks ---
    for f in [args.mpp, args.bom]:
        if not os.path.exists(f):
            print(f"❌ Error: Required input file not found at '{f}'. Halting execution.")
            return

    # Step 1: build the sliced-build file
    print(f"Building sliced build sheet from {args.mpp} and {args.bom}...")
    build_sliced_build_ID(args.mpp, args.bom, args.sliced_out)

    # Step 2: feed that file into your existing generator
    jobs = generate_jobs_from_excel(args.sliced_out)

    # Step 3: optionally persist jobs to CSV
    if jobs:
        # Ensure output directory exists
        os.makedirs(os.path.dirname(args.jobs_out) or '.', exist_ok=True)
        with open(args.jobs_out, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=jobs[0].keys())
            writer.writeheader()
            writer.writerows(jobs)
        print(f"✅ {len(jobs)} jobs written to {args.jobs_out}")

if __name__ == "__main__":
    main()
