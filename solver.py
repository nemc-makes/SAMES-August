# In MES-Demo July.24/solver.py

from ortools.sat.python import cp_model
import pandas as pd

def solve_and_extract(model, start_vars, end_vars, assigned_printer, jobs, printers, penalty_var=None):
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = 100.0  # Time limit (from previous step)

    status = solver.Solve(model)

    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        print("❌ No solution found.")
        return pd.DataFrame()

    results = []
    for jid in range(len(jobs)):
        job = jobs[jid]
        results.append({
            'job_id': job['job_id'],
            'job_title': job['job_title'],
            'start': solver.Value(start_vars[jid]),
            'end': solver.Value(end_vars[jid]),
            'duration': job['duration'],
            'printer': solver.Value(assigned_printer[jid]),
            'material': job['required_material'],
            'technology': job['required_technology'],
            'machine_model': job['machine_model'],
            'alpha_quantity_on_plate': job['alpha_quantity_on_plate']
        })

    # --- DEBUG: Printer Usage in Solution ---
    print("\n--- DEBUG: Printer Usage in Solution ---")
    used_printers_count = 0
    
    printers_used_in_solution = set()
    for jid in range(len(jobs)):
        printers_used_in_solution.add(solver.Value(assigned_printer[jid]))

    for pid, printer_spec in printers.items():
        printer_name = printer_spec['name']
        printer_type = f"{printer_spec['technology']} {printer_spec['material']}"
        
        if pid in printers_used_in_solution:
            print(f"✅ Printer {pid} ({printer_name} - {printer_type}) was USED.")
            used_printers_count += 1
        else:
            print(f"❌ Printer {pid} ({printer_name} - {printer_type}) was NOT USED.")
    print(f"Total Printers Used: {used_printers_count} out of {len(printers)}")
    
    # --- DEBUG: Location-based Penalty Value ---
    if penalty_var is not None:
        penalty_value_solved = solver.Value(penalty_var)
        print(f"DEBUG: Location-based Penalty Value: {penalty_value_solved}")
    print("------------------------------------------") 

    return pd.DataFrame(results)