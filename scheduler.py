# In MES-Demo July.24/scheduler.py

from ortools.sat.python import cp_model

def compatible_printers(printers, material, technology, machine_model): # Add machine_model parameter
    # Ensure .strip() is used on all string comparisons for robustness
    return [
        pid for pid, spec in printers.items()
        if spec['material'].strip() == material.strip() and \
           spec['technology'].strip() == technology.strip() and \
           spec['model'].strip() == machine_model.strip() # Add machine_model check
    ]

def define_job_variables(model, jobs, printers, horizon, printer_rack_id, shift_start, job_buffer_minutes=0):
    start_vars = {}
    end_vars = {}
    assigned_printer_vars = {}
    presence_literals = {}
    printer_intervals = {pid: [] for pid in printers}
    rack_vars = {}

    for jid, job in enumerate(jobs):
        # Add the global buffer to the duration for scheduling purposes.
        duration = job['duration'] + job_buffer_minutes
        
        # Pass machine_model from job data to compatible_printers
        valid_pids = compatible_printers(
            printers,
            job['required_material'],
            job['required_technology'],
            job['machine_model'] # Pass machine_model here
        )
        
        if not valid_pids:
            raise ValueError(f"No valid printers for job {jid}: {job}")
        start = model.NewIntVar(shift_start, horizon - duration, f"start_{jid}")
        end = model.NewIntVar(0, horizon, f'end_{jid}')
        printer = model.NewIntVarFromDomain(cp_model.Domain.FromValues(valid_pids), f'printer_{jid}')
        model.Add(end == start + duration)

        start_vars[jid] = start
        end_vars[jid] = end
        assigned_printer_vars[jid] = printer
        presence_literals[jid] = {}

        for pid in valid_pids:
            literal = model.NewBoolVar(f'is_j{jid}_on_p{pid}')
            presence_literals[jid][pid] = literal
            model.Add(printer == pid).OnlyEnforceIf(literal)
            model.Add(printer != pid).OnlyEnforceIf(literal.Not())
            interval = model.NewOptionalIntervalVar(start, duration, end, literal, f'int_j{jid}_p{pid}')
            printer_intervals[pid].append(interval)

        rack = model.NewIntVarFromDomain(
            cp_model.Domain.FromValues(list(set(printer_rack_id.values()))),
            f"rack_{jid}"
        )
        for pid, literal in presence_literals[jid].items():
            model.Add(rack == printer_rack_id[pid]).OnlyEnforceIf(literal)
        rack_vars[jid] = rack

    return start_vars, end_vars, assigned_printer_vars, presence_literals, printer_intervals, rack_vars

def add_no_overlap_constraints(model, printer_intervals):
    for pid, intervals in printer_intervals.items():
        if intervals:
            model.AddNoOverlap(intervals)

def add_printer_usage_variables(model, presence_literals, jobs, printers):
    printer_usage = {}
    for pid in printers:
        used = model.NewBoolVar(f'printer_{pid}_used')
        literals = [presence_literals[jid][pid] for jid in range(len(jobs)) if pid in presence_literals[jid]]
        if literals:
            model.AddBoolOr(literals).OnlyEnforceIf(used)
            model.AddBoolAnd([lit.Not() for lit in literals]).OnlyEnforceIf(used.Not())
        else:
            model.Add(used == 0)
        printer_usage[pid] = used
    return printer_usage

def build_model(printers, jobs, objective_type="minimize_makespan_and_printers", user_parameters=None, debug=False):
    model = cp_model.CpModel()
    printer_rack = {pid: printers[pid]['rack'] for pid in printers}
    rack_to_id = {rack: i for i, rack in enumerate(set(printer_rack.values()))}
    printer_rack_id = {pid: rack_to_id[rack] for pid, rack in printer_rack.items()}
    shift_start = user_parameters.get("shift_start", 480)  # 08:00 default

    shift_hours = user_parameters.get("shift_hours", 12)
    minutes_per_day = shift_hours * 60

    # === New Flexible Horizon Logic: Tailored per Material/Technology Type ===
    work_per_type = {}
    printers_per_type = {}

    # Calculate estimated work per (material, technology) type for the current batch
    for job in jobs:
        mat_tech = (job['required_material'], job['required_technology'])
        work_per_type[mat_tech] = work_per_type.get(mat_tech, 0) + job['duration']

    # Count printers per (material, technology) type
    for pid, spec in printers.items():
        mat_tech = (spec['material'], spec['technology'])
        printers_per_type[mat_tech] = printers_per_type.get(mat_tech, 0) + 1

    max_min_days_needed_per_type = 0.0
    for (material, technology), estimated_work_for_type in work_per_type.items():
        printer_count_for_type = printers_per_type.get((material, technology), 0)
        
        if printer_count_for_type > 0:
            days_needed_for_type = estimated_work_for_type / (minutes_per_day * printer_count_for_type)
            max_min_days_needed_per_type = max(max_min_days_needed_per_type, days_needed_for_type)
        else:
            print(f"WARNING: Batch contains jobs with material='{material}' and technology='{technology}', but no compatible printers are available.")
            pass 

    min_days_needed = max_min_days_needed_per_type

    if min_days_needed == 0 and len(jobs) > 0:
        min_days_needed = 0.1 

    # Set max_days buffer to 3.0 as intended
    max_days = int(min_days_needed * 3.0) + 2 
    horizon = minutes_per_day * max_days

    if debug:
        print(f"\n--- DEBUG INFO for Batch (build_model) ---")
        print(f"DEBUG: Processing batch with {len(jobs)} jobs.")

        fdm_petg_jobs = sum(1 for job in jobs if job['required_technology'] == 'FDM' and job['required_material'] == 'PETG')
        fdm_pla_jobs = sum(1 for job in jobs if job['required_technology'] == 'FDM' and job['required_material'] == 'PLA')
        lfam_petg_jobs = sum(1 for job in jobs if job['required_technology'] == 'LFAM' and job['required_material'] == 'PETG')
        print(f"DEBUG: Job distribution in this batch: FDM (PETG): {fdm_petg_jobs}, FDM (PLA): {fdm_pla_jobs}, LFAM (PETG): {lfam_petg_jobs}")
        
        print(f"📏 Estimated Work (Total for Batch): {sum(job['duration'] for job in jobs)} min")
        
        print(f"DEBUG: Work distribution by (material, technology): {work_per_type}")
        print(f"DEBUG: Printer distribution by (material, technology): {printers_per_type}")
        print(f"DEBUG: Max min days needed across all types: {max_min_days_needed_per_type:.2f}")

        print(f"🕐 Shift Duration: {minutes_per_day} min, Total Printers: {len(printers)}")
        print(f"📆 Estimated Days Needed (Based on busiest type): {min_days_needed:.2f}, Final Horizon: {horizon} min")
        print(f"--------------------------------------------")


    job_buffer_minutes = user_parameters.get("job_buffer_minutes", 0)
    if debug and job_buffer_minutes > 0:
        print(f"🛠️  Applying a buffer of {job_buffer_minutes} minutes between jobs.")

    start_vars, end_vars, assigned_printer_vars, presence_literals, printer_intervals, rack_vars = define_job_variables(
        model, jobs, printers, horizon, printer_rack_id, shift_start, job_buffer_minutes
    )

    add_no_overlap_constraints(model, printer_intervals)
    printer_usage = add_printer_usage_variables(model, presence_literals, jobs, printers)

    penalty = model.NewIntVar(0, 1000, "operator_penalty")
    penalties = []
    diff = user_parameters.get("diff", 15)
    penalty_val = user_parameters.get("penalty_val", 10)

    jobs_sorted = sorted([(jid, jobs[jid]['duration']) for jid in range(len(jobs))], key=lambda x: x[1])

    for idx_i in range(len(jobs_sorted)):
        i, dur_i = jobs_sorted[idx_i]
        for idx_j in range(idx_i + 1, min(idx_i + 20, len(jobs_sorted))):
            j, dur_j = jobs_sorted[idx_j]
            if abs(dur_i - dur_j) > 60:
                break
            if jobs[i]["required_technology"] != jobs[j]["required_technology"]:
                continue

            ei = end_vars[i]
            ej = end_vars[j]
            ri = rack_vars[i]
            rj = rack_vars[j]

            time_diff = model.NewIntVar(0, horizon, f"abs_diff_{i}_{j}")
            model.AddAbsEquality(time_diff, ei - ej)

            close_end = model.NewBoolVar(f"close_end_{i}_{j}")
            model.Add(time_diff <= diff).OnlyEnforceIf(close_end)
            model.Add(time_diff > diff).OnlyEnforceIf(close_end.Not())

            diff_rack = model.NewBoolVar(f"diff_rack_{i}_{j}")
            model.Add(ri != rj).OnlyEnforceIf(diff_rack)
            model.Add(ri == rj).OnlyEnforceIf(diff_rack.Not())

            penalized = model.NewBoolVar(f"penalized_{i}_{j}")
            model.AddBoolAnd([close_end, diff_rack]).OnlyEnforceIf(penalized)
            penalties.append(penalized)

    model.Add(penalty == sum(penalties))

    printer_job_counts = {}
    max_job_count = model.NewIntVar(0, len(jobs), "max_job_count")
    for pid in printers:
        printer_assignments = [presence_literals[jid][pid] for jid in presence_literals if pid in presence_literals[jid]]
        count = model.NewIntVar(0, len(jobs), f"job_count_p{pid}")
        model.Add(count == sum(printer_assignments))
        printer_job_counts[pid] = count
    model.AddMaxEquality(max_job_count, list(printer_job_counts.values()))

    load_penalty_weight = 30

    # NOTE: end_vars from define_job_variables now includes the buffer time.
    # To get the true makespan for the objective function, we must calculate it
    # from the start time and the original, non-buffered job duration.
    makespan = model.NewIntVar(0, horizon, 'makespan')
    true_end_times = []
    for jid, job in enumerate(jobs):
        true_end = model.NewIntVar(0, horizon, f"true_end_{jid}")
        model.Add(true_end == start_vars[jid] + job['duration'])
        true_end_times.append(true_end)

    if not true_end_times: # Handle case where there are no jobs
        model.Add(makespan == 0)
    else:
        model.AddMaxEquality(makespan, true_end_times)

    total_start_time = model.NewIntVar(0, horizon * len(jobs), "total_start_time")
    model.Add(total_start_time == sum(start_vars.values()))

    if objective_type == "operator_aware_composite":
        model.Minimize(
            makespan +
            penalty_val * penalty +
            load_penalty_weight * max_job_count +
            1 * total_start_time
        )
    else:
        model.Minimize(
            makespan * (len(printer_usage) + 1) +
            sum(printer_usage.values()) +
            total_start_time
        )

    return model, start_vars, end_vars, assigned_printer_vars, penalty