#!/usr/bin/env python3
import pandas as pd
import math
from datetime import datetime
from jobgen import generate_jobs_from_excel

# ─── CONFIG ────────────────────────────────────────────────────────────────
MPP_FILE        = "MPP Data 13 Easels.csv"
BOM_FILE        = "Easel Components BOM.csv"
SLICED_FILE     = "Sliced Build Generated.xlsx"
JOBS_OUTPUT_CSV = "Jobs_Output.csv"
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
    # Step 1: build the sliced-build file
    build_sliced_build_ID(MPP_FILE, BOM_FILE, SLICED_FILE)

    # Step 2: feed that file into your existing generator
    jobs = generate_jobs_from_excel(SLICED_FILE)

    # Step 3: optionally persist jobs to CSV
    if jobs:
        with open(JOBS_OUTPUT_CSV, 'w', newline='') as f:
            import csv
            writer = csv.DictWriter(f, fieldnames=jobs[0].keys())
            writer.writeheader()
            writer.writerows(jobs)
        print(f"✅ {len(jobs)} jobs written to {JOBS_OUTPUT_CSV}")

if __name__ == "__main__":
    main()
