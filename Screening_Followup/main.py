import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import tkinter as tk
from tkinter import ttk, messagebox

# ======================================================
# CONFIGURATION
# ======================================================
SERVICE_ACCOUNT_FILE = "screeningfollowup-4a463d7d64cb.json"
SHEET_ID = "1rA6u8Z03Tq9icAAIGP1Ki6FA9U_wFKjsKaizYLkMb_M"
SHEET_TAB_NAME = "SCREENING"
OUTPUT_FILE = r"D:\matching_harsh\Screening_Followup\input\input1.xlsx"

FINPLOY_GREEN = "#4EA647"
FINPLOY_BLUE = "#206A98"
BACKGROUND = "#FFFFFF"

REMARK_OPTIONS = [
    "Drop", "Hold", "Ignore", "Interested", "Lineup",
    "Location Not Available", "Not Interested", "Remark",
    "Ringing", "Switchoff", "Duplicate"
]

# ======================================================
# CONNECT TO GOOGLE SHEET
# ======================================================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
gc = gspread.authorize(creds)

try:
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(SHEET_TAB_NAME)
    data = ws.get_all_records()
except Exception as e:
    root = tk.Tk()
    root.withdraw()
    messagebox.showerror("Error", f"Unable to read Google Sheet:\n{e}")
    raise SystemExit

# ======================================================
# CONVERT TO DATAFRAME
# ======================================================
df = pd.DataFrame(data)
df.columns = [c.strip() for c in df.columns]

# Identify columns
remark_col = next((c for c in df.columns if "remark" in c.lower()), None)
contact_col = next((c for c in df.columns if "contact" in c.lower() or "mobile" in c.lower() or "phone" in c.lower()), None)

if not remark_col or not contact_col:
    root = tk.Tk()
    root.withdraw()
    messagebox.showerror("Error", "Could not find 'Remark' or 'Contact' column in the sheet.")
    raise SystemExit

# ======================================================
# FILTER & DEDUPLICATE
# ======================================================
total_candidates = len(df)
ringing_df = df[df[remark_col].astype(str).str.strip().str.lower() == "ringing"]
ringing_df = ringing_df.drop_duplicates(subset=[contact_col])
ringing_count = len(ringing_df)
ringing_df.to_excel(OUTPUT_FILE, index=False)

# ======================================================
# COUNT REMARK OPTIONS (robust)
# ======================================================
remark_counts = {r: 0 for r in REMARK_OPTIONS}
for val in df[remark_col]:
    if pd.isna(val):
        continue
    clean = str(val).strip()
    if clean in remark_counts:
        remark_counts[clean] += 1
    else:
        remark_counts[clean] = remark_counts.get(clean, 0) + 1

# ======================================================
# GUI DESIGN
# ======================================================
root = tk.Tk()
root.title("Finploy Tracker Summary")
root.geometry("480x550")
root.configure(bg=BACKGROUND)
root.resizable(False, False)

# ---------------- Header ----------------
header = tk.Label(
    root,
    text="Finploy Tracker Summary",
    font=("Libre Baskerville", 18, "bold"),
    fg=FINPLOY_BLUE,
    bg=BACKGROUND,
)
header.pack(pady=(20, 10))

# ---------------- Key Metrics ----------------
frame_metrics = tk.Frame(root, bg=BACKGROUND)
frame_metrics.pack(pady=10)

tk.Label(frame_metrics, text="Total Candidates:", font=("Arial", 12, "bold"), bg=BACKGROUND, fg="#444").grid(row=0, column=0, sticky="w", padx=5)
tk.Label(frame_metrics, text=str(total_candidates), font=("Arial", 12, "bold"), fg=FINPLOY_GREEN, bg=BACKGROUND).grid(row=0, column=1, sticky="e", padx=5)

tk.Label(frame_metrics, text="Ringing Candidates:", font=("Arial", 12, "bold"), bg=BACKGROUND, fg="#444").grid(row=1, column=0, sticky="w", padx=5, pady=(4,0))
tk.Label(frame_metrics, text=str(ringing_count), font=("Arial", 12, "bold"), fg=FINPLOY_BLUE, bg=BACKGROUND).grid(row=1, column=1, sticky="e", padx=5, pady=(4,0))

# ---------------- Separator ----------------
ttk.Separator(root, orient='horizontal').pack(fill='x', padx=20, pady=10)

# ---------------- Scrollable Frame for Remarks ----------------
container = tk.Frame(root, bg=BACKGROUND)
canvas = tk.Canvas(container, bg=BACKGROUND, highlightthickness=0)
scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
scrollable_frame = tk.Frame(canvas, bg=BACKGROUND)

scrollable_frame.bind(
    "<Configure>",
    lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
)

canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
canvas.configure(yscrollcommand=scrollbar.set)

container.pack(fill="both", expand=True, padx=25, pady=(0, 10))
canvas.pack(side="left", fill="both", expand=True)
scrollbar.pack(side="right", fill="y")

# ---------------- Remark Count Table ----------------
tk.Label(scrollable_frame, text="Status Counts", font=("Arial", 13, "bold"), fg="#222", bg=BACKGROUND).pack(anchor="w", pady=(0,8))
for status, count in remark_counts.items():
    row = tk.Frame(scrollable_frame, bg=BACKGROUND)
    row.pack(fill="x", pady=1)
    tk.Label(row, text=f"{status:25}", font=("Arial", 11), bg=BACKGROUND, fg="#333", anchor="w", width=25).pack(side="left")
    tk.Label(row, text=str(count), font=("Arial", 11, "bold"), bg=BACKGROUND, fg=FINPLOY_GREEN, anchor="e").pack(side="right")

# ---------------- Footer ----------------
ttk.Separator(root, orient='horizontal').pack(fill='x', padx=20, pady=(10,5))
footer = tk.Label(
    root,
    text="Data synced from Google Sheets • Saved as input1.xlsx",
    font=("Arial", 9),
    fg="#777",
    bg=BACKGROUND
)
footer.pack(pady=(0, 10))

# ---------------- Run GUI ----------------
root.mainloop()
print("✅ File saved as input1.xlsx and summary shown.")

try:
    import subprocess
    print("▶️ Running main2.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Screening_Followup\main2.py"], check=True)
    print("✅ main2.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main2.py: {e}")