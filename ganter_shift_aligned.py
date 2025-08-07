import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.offsetbox import AnchoredText
from matplotlib.ticker import FuncFormatter
from datetime import datetime, timedelta
import os
from printerconfig import get_printers

# === Load printer names ===
PRINTER_NAMES = {pid: info.get("name", f"Printer {pid}") for pid, info in get_printers().items()}

def plot_gantt(df, title, output_csv='schedules/operator_aware_composite.csv'):
    if df.empty:
        print("❌ No data to plot.")
        return

    # === Infer shift start from data ===
    SHIFT_START_MINUTES = df["start_time"].min() // 60 * 60  # round down to hour
    DAY_START_TIME = datetime.strptime(f"{SHIFT_START_MINUTES // 60:02d}:00", "%H:%M")
    WORKDAY_MINUTES = 12 * 60  # assume 12-hour shifts for layout

    def get_day_block(minute):
        return (minute - SHIFT_START_MINUTES) // WORKDAY_MINUTES, (minute - SHIFT_START_MINUTES) % WORKDAY_MINUTES

    def time_formatter(x, _):
        day = int(x) // WORKDAY_MINUTES + 1
        minutes_into_day = int(x) % WORKDAY_MINUTES
        time = DAY_START_TIME + timedelta(minutes=minutes_into_day)
        return time.strftime('%H:%M')

    printers = sorted(df['printer'].unique())
    printer_indices = {pid: i for i, pid in enumerate(printers)}
    color_map = {pid: plt.get_cmap('tab10')(i % 10) for i, pid in enumerate(printers)}

    fig, ax = plt.subplots(figsize=(16, 10))
    schedule_export = []

    for _, row in df.iterrows():
        job_id = row["job_id"]
        printer = row["printer"]
        start = row.get("absolute_start_time", row["start_time"])  # ✅ Prefer absolute_start_time
        end = row["end_time"]
        duration = end - start
        job_title = row["job_title"]
        printer_name = PRINTER_NAMES.get(printer, f"Printer {printer}")

        if start < SHIFT_START_MINUTES:
            continue  # Skip jobs before shift start

        day_start_idx, minute_start_in_day = get_day_block(start)
        day_end_idx, _ = get_day_block(end)

        if day_start_idx == day_end_idx:
            y = printer_indices[printer] + day_start_idx * (len(printers) + 1)
            ax.barh(y, duration, left=minute_start_in_day, height=0.6, color=color_map[printer])
            ax.text(minute_start_in_day + 2, y, job_title, va='center', fontsize=8)
            schedule_export.append({
                'job_id': job_id,
                'printer': printer,
                'printer_name': printer_name,
                'job_title': job_title,
                'start_time': (DAY_START_TIME + timedelta(minutes=start % WORKDAY_MINUTES)).strftime('%H:%M'),
                'end_time': (DAY_START_TIME + timedelta(minutes=end % WORKDAY_MINUTES)).strftime('%H:%M'),
                'day': f"Day {day_start_idx + 1}"
            })
        else:
            remaining = duration
            current_start = start
            while remaining > 0:
                current_day, minute_in_day = get_day_block(current_start)
                y = printer_indices[printer] + current_day * (len(printers) + 1)
                max_in_day = WORKDAY_MINUTES - minute_in_day
                this_duration = min(remaining, max_in_day)

                ax.barh(y, this_duration, left=minute_in_day, height=0.6, color=color_map[printer])
                ax.text(minute_in_day + 2, y, f"{job_title} (cont.)", va='center', fontsize=7)

                schedule_export.append({
                    'job_id': job_id,
                    'printer': printer,
                    'printer_name': printer_name,
                    'job_title': job_title,
                    'start_time': (DAY_START_TIME + timedelta(minutes=current_start % WORKDAY_MINUTES)).strftime('%H:%M'),
                    'end_time': (DAY_START_TIME + timedelta(minutes=(current_start + this_duration) % WORKDAY_MINUTES)).strftime('%H:%M'),
                    'day': f"Day {current_day + 1}"
                })

                remaining -= this_duration
                current_start += this_duration

    total_days = max(df["end_time"]) // WORKDAY_MINUTES + 1
    y_ticks, y_labels = [], []
    for day in range(total_days):
        for pid in printers:
            y = printer_indices[pid] + day * (len(printers) + 1)
            label = PRINTER_NAMES.get(pid, f"Printer {pid}")
            y_ticks.append(y)
            y_labels.append(f"Day {day + 1} – {label}")

    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels, fontsize=9)
    ax.set_xlabel("Time of Day")
    ax.set_title(f"Gantt Chart – {title}")
    ax.set_xlim(0, WORKDAY_MINUTES)
    ax.xaxis.set_major_formatter(FuncFormatter(time_formatter))
    ax.grid(True, axis='x', linestyle='--', color='gray', alpha=0.4)

    for d in range(1, total_days):
        ax.axvline(x=d * WORKDAY_MINUTES, color='gray', linestyle='--', linewidth=0.5)

    legend = AnchoredText(
        f"• {len(df)} jobs across {total_days} day(s)\n"
        f"• Y = Day & Printer, X = Time ({DAY_START_TIME.strftime('%H:%M')} onward)\n"
        f"• Jobs split over days if needed\n"
        f"• Jobs before shift start skipped",
        loc='upper right', prop=dict(size=9), frameon=True
    )
    legend.patch.set_facecolor('whitesmoke')
    legend.patch.set_alpha(0.95)
    ax.add_artist(legend)

    plt.tight_layout(pad=2.0)
    plt.show()

    pd.DataFrame(schedule_export).to_csv(output_csv, index=False)
    print(f"✅ Exported formatted schedule to {output_csv}")


if __name__ == "__main__":
    path = "schedules/operator_aware_composite.csv"
    if not os.path.exists(path):
        print(f"❌ File not found: {path}")
        exit()

    df = pd.read_csv(path)

    def time_to_minutes(t):
        if isinstance(t, str):
            h, m = map(int, t.split(":"))
            return h * 60 + m
        return t

    df["start_time"] = df["start_time"].apply(time_to_minutes)
    df["end_time"] = df["end_time"].apply(time_to_minutes)

    plot_gantt(df, "Operator Aware Composite")
