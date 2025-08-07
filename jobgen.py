# In MES-Demo July.24/jobgen.py

import warnings
import pandas as pd
import math

def generate_jobs_from_excel(filepath="Sliced Build Generated.xlsx"):
    # 1. Load
    if filepath.lower().endswith(".xlsx"):
        df = pd.read_excel(filepath)
    else:
        df = pd.read_csv(filepath)

    # 2. Normalize headers: strip whitespace, replace spaces with underscores
    df.columns = [c.strip().replace(' ', '_') for c in df.columns]
    print("🧐 Columns loaded for job gen:", df.columns.tolist())

    # 3. Default Status and filter
    if 'Status' not in df.columns:
        df['Status'] = 'Queued'
    df = df[df['Status'].str.lower() == 'queued']

   # 4. Ensure Quantity_of_Runs exists, otherwise default to one
    if 'Quantity_of_Runs' not in df.columns:
        warnings.warn(
            f"No 'Quantity_of_Runs' column found in {df.columns.tolist()}; "
            "defaulting all runs to 1."
    )
        df['Quantity_of_Runs'] = 1

    # 5. Explode into job dicts
    jobs = []
    job_id = 0
    for _, row in df.iterrows():
        # use the underscore name here:
        qty = int(row['Quantity_of_Runs']) if not pd.isna(row['Quantity_of_Runs']) else 1

        # same for duration:
        if 'Estimated_Print_Time_Minutes' in df.columns and not pd.isna(row['Estimated_Print_Time_Minutes']):
            dur = int(row['Estimated_Print_Time_Minutes'])
        else:
            dur = 30

        # material & tech—use underscore names
        raw_mat = row.get('Required_Material_ID','')
        material = raw_mat.strip() if isinstance(raw_mat, str) else ''
        raw_tech = row.get('Technology','')
        tech     = raw_tech.strip()   if isinstance(raw_tech, str) else ''

        sliced_id = row.get('Sliced_Build_ID','').strip()
        
        machine_model = row.get('Machine_Model', '').strip() 

        # Capture Alpha_Quantity_on_Plate from the input DataFrame
        alpha_qty_on_plate = row.get('Alpha_Quantity_on_Plate', 1) 

        for i in range(qty):
            jobs.append({
                'job_id': job_id,
                'job_title': f"{sliced_id}_R{i+1}",
                'required_material': material,
                'required_technology': tech,
                'duration': dur,
                'material': material, 
                'technology': tech,   
                'machine_model': machine_model,
                'alpha_quantity_on_plate': alpha_qty_on_plate 
            })
            job_id += 1

    return jobs