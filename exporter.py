# In MES-Demo July.24/exporter.py

import pandas as pd
import os
from datetime import datetime, timedelta
from printerconfig import get_printers

def export_schedule(df, label, output_dir="schedules", shift_start_hour=8, schedule_start_date=None):
    os.makedirs(output_dir, exist_ok=True)

    # Maps for printer name and rack
    printers = get_printers()
    printer_rack_map = {pid: p['rack'] for pid, p in printers.items()}
    printer_name_map = {pid: p.get("name", f"Printer {pid}") for pid, p in printers.items()}

    # Base date calculations using the PASSED schedule_start_date
    if schedule_start_date is None:
        base_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        print("WARNING: schedule_start_date not provided to export_schedule. Defaulting to today's date.")
    else:
        base_date = schedule_start_date.replace(hour=0, minute=0, second=0, microsecond=0)


    shift_start_time = base_date + timedelta(hours=shift_start_hour)

    # Add derived columns
    if 'printer' in df.columns:
        df["printer_name"] = df["printer"].map(printer_name_map)
        df["rack"] = df["printer"].map(printer_rack_map)

    if 'start' in df.columns:
        df["start_datetime"] = df["start"].apply(lambda m: base_date + timedelta(minutes=m))
        df["end_datetime"] = df["end"].apply(lambda m: base_date + timedelta(minutes=m))

    for col in ["batch_number", "technology", "material", "machine_model", "alpha_quantity_on_plate"]:
        if col not in df.columns:
            df[col] = None
            
    export_cols = [
        "job_id", "job_title", "batch_number", "technology", "material",
        "printer", "printer_name", "rack",
        "start", "end", "start_datetime", "end_datetime",
        "objective",
        "machine_model",
        "alpha_quantity_on_plate"
    ]
    actual_export_cols = [col for col in export_cols if col in df.columns]
    export_df = df[actual_export_cols]

    # --- NEW FILENAME LOGIC (Corrected to desired format) ---
    # Extract month and day from schedule_start_date (base_date) for the "for_MM-DD" part
    schedule_md = base_date.strftime("%m-%d")
    # Extract current date AND TIME (hour, minute, second) for creation timestamp to ensure uniqueness
    creation_timestamp_full = datetime.now().strftime("%m-%d-%y_%H-%M-%S")

    # Construct the new filename
    filename = f"{label.replace(' ', '_').lower()}_for_{schedule_md}_created_{creation_timestamp_full}.csv"
    # --- END NEW FILENAME LOGIC ---

    # === Save to local schedules folder ===
    local_path = os.path.join(output_dir, filename)
    export_df.to_csv(local_path, index=False)
    print(f"✅ Exported to local path: {local_path}")