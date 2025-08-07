#!/usr/bin/env python3
import os
import argparse
import pandas as pd
import math
from datetime import datetime

# Domain imports
from printerconfig import get_printers
from jobgen import generate_jobs_from_excel
from scheduler import build_model
from solver import solve_and_extract
from exporter import export_schedule

# --- CONFIG: Input/Output File Paths --------------------------------
# NOTE: These paths are relative to the project root.
INPUT_DIR = "Data Input"
OUTPUT_DIR = "schedules"

MPP_FILE        = os.path.join(INPUT_DIR, "MPP Data 2 Private Offices.csv")
BOM_FILE        = os.path.join(INPUT_DIR, "20250721_PO_Rebuild Components BOM.csv")
SLICED_FILE     = os.path.join(INPUT_DIR, "Sliced Build Generated.xlsx")
JOBS_OUTPUT_CSV = os.path.join(OUTPUT_DIR, "generated_jobs.csv")
# --------------------------------------------------------------------

# --- ETL: build sliced-build sheet from BOM + MPP -----------------
def build_sliced_build_ID(mpp_path: str, bom_path: str, out_path: str):
    df_mpp = pd.read_csv(mpp_path, parse_dates=['Overall_Due_Date'])
    df_bom = pd.read_csv(bom_path)

    # Normalize BOM Product_SKU formatting: convert dashes to underscores to match MPP
    df_bom['Product_SKU'] = df_bom['Product_SKU'].str.replace('-', '_')

    # Merge BOM with MPP to get per-MPP-item quantities and associated MPP details
    df_merged = df_bom.merge(
        df_mpp[['MPP_Item_ID','Product_SKU','Overall_Due_Date','Status','Project_Phase']],
        on='Product_SKU',
        how='inner'
    )

    # --- AGGREGATED DEMAND LOGIC: Consolidate total demand per Part_Name across all MPP items ---
    aggregated_parts_data = {} 

    for _, r in df_merged.iterrows():
        part_name = r['Part_Name']
        quantity_per_product_unit = r['Print_Quantity']
        
        # Determine current material ID from the row, stripping whitespace
        current_material_id = r.get('Required_Material_ID', '').strip() 
        
        if part_name not in aggregated_parts_data:
            # Initialize data for a new part_name, including initial values for new fields
            aggregated_parts_data[part_name] = {
                'total_quantity_needed': 0,
                'Alpha_Quantity_on_Plate': r['Alpha_Quantity_on_Plate'],
                'Duration': r['Duration'], # Assuming duration is consistent per part
                'Printing_Method': r['Printing_Method'],
                'Required_Material_ID': current_material_id, 
                'Machine_Model': r.get('Machine_Model', ''),
                'earliest_due_date': r['Overall_Due_Date'],
                'project_phases': set()
            }
        
        aggregated_parts_data[part_name]['total_quantity_needed'] += quantity_per_product_unit
        
        aggregated_parts_data[part_name]['earliest_due_date'] = min(
            aggregated_parts_data[part_name]['earliest_due_date'],
            r['Overall_Due_Date']
        )
        
        aggregated_parts_data[part_name]['project_phases'].add(r['Project_Phase'])


    rows = []
    for part_name, data in aggregated_parts_data.items():
        total_quantity_needed = data['total_quantity_needed']
        alpha_quantity_on_plate = data['Alpha_Quantity_on_Plate']
        
        number_of_runs_for_part = math.ceil(total_quantity_needed / alpha_quantity_on_plate)

        # Handle potential missing or invalid Duration values
        raw_duration = data['Duration']
        try:
            timedelta_duration = pd.to_timedelta(raw_duration)
            if pd.isna(timedelta_duration): 
                estimated_print_time_minutes = 30 
                print(f"WARNING: Duration for part '{part_name}' is invalid or missing (after conversion). Using default of {estimated_print_time_minutes} minutes.")
            else:
                estimated_print_time_minutes = int(timedelta_duration.total_seconds() / 60)
        except Exception as e:
            estimated_print_time_minutes = 30
            print(f"WARNING: Duration for part '{part_name}' caused error: '{e}'. Using default of {estimated_print_time_minutes} minutes.")

        # Apply material inference logic more robustly
        final_material_id = data['Required_Material_ID']
        if not final_material_id.strip():
            if data['Printing_Method'] == 'LFAM':
                final_material_id = 'PETG'
            elif data['Printing_Method'] == 'FDM':
                final_material_id = 'PETG'
        
        formatted_due_date = data['earliest_due_date'].strftime('%Y-%m-%d') if pd.notna(data['earliest_due_date']) else ''
        formatted_project_phases = ", ".join(sorted(list(data['project_phases'])))

        for run_idx in range(1, number_of_runs_for_part + 1):
            rows.append({
                'Sliced Build ID': f"MPP_ALL__{part_name}_R{run_idx}", 
                'Quantity of Runs': 1,
                'Alpha Quantity on Plate': alpha_quantity_on_plate,
                'Total Required Quantity': total_quantity_needed, 
                'Estimated Print Time Minutes': estimated_print_time_minutes,
                'Required Material ID': final_material_id,
                'Technology': data['Printing_Method'],
                'Status': 'Queued', 
                'Created Timestamp': datetime.now().isoformat(),
                'Overall_Due_Date': formatted_due_date,
                'Project_Phase': formatted_project_phases,
                'Machine_Model': data['Machine_Model']
            })

    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    pd.DataFrame(rows).to_excel(out_path, index=False)
    print(f"✅ Sliced build sheet written to {out_path} ({len(rows)} rows)")


def main():
    # --- Pre-flight Checks ---
    for f in [MPP_FILE, BOM_FILE]:
        if not os.path.exists(f):
            print(f"❌ Error: Required input file not found at '{f}'. Halting execution.")
            return

    # Step 1: Build sliced-build sheet
    print(f"Building sliced build sheet from {MPP_FILE} and {BOM_FILE}...")
    build_sliced_build_ID(MPP_FILE, BOM_FILE, SLICED_FILE)

    # --- Post-build Check ---
    if not os.path.exists(SLICED_FILE):
        print(f"❌ Error: Sliced build file was not created at '{SLICED_FILE}'. Halting execution.")
        return

    # Step 2: Generate jobs from sliced sheet
    print(f"Generating jobs from {SLICED_FILE}...")
    printers = get_printers()
    jobs = generate_jobs_from_excel(SLICED_FILE) 
    
    # === Setup command-line argument parsing ===
    parser = argparse.ArgumentParser(description="Run the MES scheduling system.")
    parser.add_argument(
        '--start-date',
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help='The start date for the schedule in YYYY-MM-DD format. Defaults to today.'
    )
    parser.add_argument('--shift-start-hour', type=int, default=8, help='The hour the shift starts (e.g., 8 for 8:00 AM).')
    parser.add_argument('--shift-length', type=int, default=12, help='The length of a single shift in hours.')
    parser.add_argument('--stagger', type=int, default=30, help="Minutes apart for finishes to be considered 'too close'.")
    parser.add_argument('--penalty', type=int, default=5, help='Penalty value for close finishes on different racks.')
    parser.add_argument('--buffer', type=int, default=0, help='Fixed gap in minutes between jobs.')
    parser.add_argument('--debug', action='store_true', help='Enable debug output.')
    
    args = parser.parse_args()

    # === Process arguments and set up parameters ===
    print("\n🧠 Running Batched Operator-Aware Scheduling with provided parameters...")
    try:
        schedule_start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)
    except ValueError:
        print(f"❌ Invalid date format for --start-date: '{args.start_date}'. Please use YYYY-MM-DD. Exiting.")
        return

    shift_start_hour = args.shift_start_hour
    shift_length_hours = args.shift_length
    stagger_minutes = args.stagger
    penalty_value = args.penalty
    job_buffer_minutes = args.buffer
    debug = args.debug

    shift_start = shift_start_hour * 60
    shift_length = shift_length_hours * 60
    # === End parameter setup ===

    # Persist generated jobs using export_schedule for consistency and OneDrive sync
    # Pass output_dir for the specific subfolder
    export_schedule(pd.DataFrame(jobs), 'generated_jobs', 
                    output_dir=os.path.dirname(JOBS_OUTPUT_CSV), 
                    shift_start_hour=shift_start_hour, 
                    schedule_start_date=schedule_start_date) 
    print(f"✅ {len(jobs)} jobs written to {JOBS_OUTPUT_CSV}") 

    # Define batching window
    target_minutes_per_batch = shift_length * max(1, len(printers))
    print(f"📦 Target job time per batch: {target_minutes_per_batch} minutes")

    # Greedy batching by job_title
    jobs_sorted = sorted(jobs, key=lambda j: j['job_title'])
    batches = []
    current_batch = []
    current_total = 0

    for job in jobs_sorted:
        if current_total + job['duration'] <= target_minutes_per_batch:
            current_batch.append(job)
            current_total += job['duration']
        else:
            batches.append(current_batch)
            current_batch = [job]
            current_total = job['duration']
    if current_batch:
        batches.append(current_batch)

    print(f"📊 Total Batches: {len(batches)}")

    # Solve each batch
    results, unscheduled, offset = [], [], 0
    for idx, batch_jobs in enumerate(batches, start=1):
        print(f"\n🚀 Solving batch {idx}/{len(batches)} with {len(batch_jobs)} jobs")
        params = {
            'shift_start': shift_start,
            'shift_hours': shift_length_hours,
            'diff': stagger_minutes,
            'penalty_val': penalty_value,
            'job_buffer_minutes' : job_buffer_minutes
        }
        model, starts, ends, assigns, penalty_var = build_model(
            printers, batch_jobs, 'operator_aware_composite', params, debug=debug
        )
        df_batch = solve_and_extract(model, starts, ends, assigns, batch_jobs, printers, penalty_var)
        if df_batch.empty:
            print(f"❌ Batch {idx} failed; adding to unscheduled pool.")
            unscheduled.extend(batch_jobs)
            continue
        # Attach metadata
        df_batch['batch_number'] = idx
        df_batch['technology'] = [j['required_technology'] for j in batch_jobs]
        df_batch['material']   = [j['required_material'] for j in batch_jobs]
        # Offset times
        df_batch['start'] += offset
        df_batch['end']   += offset
        results.append(df_batch)
        offset += shift_length * 2

    # Save schedule outputs
    if results:
        full = pd.concat(results, ignore_index=True)
        full['objective'] = 'operator_aware_composite'
        export_schedule(full, 'operator_aware_composite', 
                        output_dir="schedules", 
                        shift_start_hour=shift_start_hour, 
                        schedule_start_date=schedule_start_date)
        print(f"\n✅ Combined schedule saved to schedules/combined_results.csv")
        if unscheduled:
            unsched_path = os.path.join(os.path.dirname(JOBS_OUTPUT_CSV), 'unscheduled_jobs.csv') 
            pd.DataFrame(unscheduled).to_csv(unsched_path, index=False)
            print(f"⚠️  {len(unscheduled)} jobs unscheduled; saved to {unsched_path}")
    else:
        print("\n❌ No batches succeeded.")

if __name__ == '__main__':
    main()