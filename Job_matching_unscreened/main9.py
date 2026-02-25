import pandas as pd
import os
import customtkinter as ctk
from tkinter import messagebox
import subprocess

# -------------------------------
# Phase 2.2 – Data Cleaning & Activity Tracking Script (Updated)
# -------------------------------

# Input and Output files
input_file = r"D:\matching_harsh\Job_matching_unscreened\output\output1.xlsx"
OUTPUT_DIR = r"D:\matching_harsh\Job_matching_unscreened\output"
output_file = os.path.join(OUTPUT_DIR, "output2.xlsx")

# Load the Excel file
df = pd.read_excel(input_file)

# -------------------------------
# Task 1 – Add 'name_location' column
# -------------------------------
if 'name_location' not in df.columns:
    df.insert(5, 'name_location', df.iloc[:, 0].astype(str) + df.iloc[:, 4].astype(str))

# -------------------------------
# Task 2 – Add 'clean_salary' column
# -------------------------------
salary_col_index = 3
salary_series = df.iloc[:, salary_col_index].astype(str)

salary_series = (
    salary_series.str.replace('₹', '', regex=False)
                 .str.replace('$', '', regex=False)
                 .str.replace(',', '', regex=False)
                 .str.replace('Lacs', '', regex=False)
                 .str.strip()
)

def safe_float(s):
    try:
        return float(s)
    except:
        return 0.0

salary_series = salary_series.apply(safe_float)

def convert_to_lacs(value):
    if value > 1000:
        return round(value / 100000, 2)
    return round(value, 2)

clean_salary = salary_series.apply(convert_to_lacs)

if 'clean_salary' in df.columns:
    df['clean_salary'] = clean_salary
else:
    if 'employment-detail' in df.columns:
        insert_index = df.columns.get_loc('employment-detail') + 1
    else:
        insert_index = 7
    df.insert(insert_index, 'clean_salary', clean_salary)

# -------------------------------
# Task 4 – Rename Columns
# -------------------------------
rename_map = {}
if 'link' in df.columns:
    rename_map['link'] = 'name of candidate'
if 'meta-data' in df.columns:
    rename_map['meta-data'] = 'experience'
df.rename(columns=rename_map, inplace=True)

# -------------------------------
# Task 5 – Extract Modification + Activity
# -------------------------------
mod_list = []
act_list = []

for idx, row in df.iterrows():
    mod_value = 'NA'
    act_value = 'NA'
    for val in row:
        val_str = str(val)
        if val_str.lower().startswith("modified"):
            mod_value = val_str
            break
    for val in row:
        val_str = str(val)
        if val_str.lower().startswith(("active", "currently")):
            act_value = val_str
            break
    mod_list.append(mod_value)
    act_list.append(act_value)

df['Modification'] = mod_list
df['Activity'] = act_list

# -------------------------------
# Task 6 – Ensure metadata columns
# -------------------------------
metadata_cols = [
    'finploy_id', 'area', 'city', 'state', 'city_id',
    'department', 'product', 'composit_key'
]

for col in metadata_cols:
    if col not in df.columns:
        df[col] = ''

# -------------------------------
# GUI for Department + Product
# -------------------------------
ctk.set_appearance_mode("System")
ctk.set_default_color_theme("blue")

class InputApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Finploy - Department & Product Input")
        self.geometry("400x300")
        self.resizable(False, False)

        title_label = ctk.CTkLabel(self, text="Enter Department and Product", font=("Helvetica", 18, "bold"))
        title_label.pack(pady=(30, 20))

        self.dept_var = ctk.StringVar()
        dept_label = ctk.CTkLabel(self, text="Department:")
        dept_label.pack(anchor="w", padx=60)
        self.dept_entry = ctk.CTkEntry(self, textvariable=self.dept_var, width=250)
        self.dept_entry.pack(pady=(0, 20))
        self.dept_entry.bind("<Return>", self.focus_next_entry)

        self.prod_var = ctk.StringVar()
        prod_label = ctk.CTkLabel(self, text="Product:")
        prod_label.pack(anchor="w", padx=60)
        self.prod_entry = ctk.CTkEntry(self, textvariable=self.prod_var, width=250)
        self.prod_entry.pack(pady=(0, 30))
        self.prod_entry.bind("<Return>", self.trigger_submit)

        self.submit_btn = ctk.CTkButton(self, text="Apply & Save", command=self.submit)
        self.submit_btn.pack()

        self.dept_entry.focus_set()

    def focus_next_entry(self, event):
        self.prod_entry.focus_set()

    def trigger_submit(self, event):
        self.submit_btn.invoke()

    def submit(self):
        dept = self.dept_var.get().strip()
        prod = self.prod_var.get().strip()
        if not dept or not prod:
            messagebox.showerror("Missing Data", "Please enter both Department and Product.")
            return
        self.destroy()
        global DEPT_VALUE, PROD_VALUE
        DEPT_VALUE, PROD_VALUE = dept, prod

app = InputApp()
app.mainloop()

# -------------------------------
# Apply Input to DF
# -------------------------------
df['department'] = DEPT_VALUE
df['product'] = PROD_VALUE

print(f"✅ Department set as: {DEPT_VALUE}")
print(f"✅ Product set as: {PROD_VALUE}")

# -------------------------------
# Task 7 – Reorder columns
# -------------------------------
desired_order = [
    'name of candidate', 'link href', 'experience', 'meta-data 2', 'location',
    'name_location', 'employment-detail', 'designation','company','clean_salary',
    'Modification', 'Activity',
    'finploy_id', 'area', 'city', 'state', 'city_id', 'department', 'product', 'composit_key'
]

remaining_cols = [col for col in df.columns if col not in desired_order]
final_order = desired_order + remaining_cols
df = df[final_order]

# -------------------------------
# Save Output
# -------------------------------
df.to_excel(output_file, index=False)
messagebox.showinfo(
    "Success",
    f"File saved successfully!\n\nDepartment: {DEPT_VALUE}\nProduct: {PROD_VALUE}\n\nSaved as:\n{output_file}"
)
print(f"✅ File saved successfully at: {output_file}")

# ============================================================
# 🚀 RUN TWO SCRIPTS **IN PARALLEL**
# ============================================================

print("\n🚀 Launching background processes...\n")

# 1️⃣ UNSCREENED → main10.py
unscreened_script = r"D:\matching_harsh\Job_matching_unscreened\main10.py"
p1 = subprocess.Popen(["python", unscreened_script])
print("▶️ Started main10.py (unscreened)")

# 2️⃣ SCREENED → main.py
screened_script = r"D:\matching_harsh\Job_matching_Screened\main.py"
p2 = subprocess.Popen(["python", screened_script])
print("▶️ Started Screened main.py")

print("\n⏳ Waiting for both scripts to finish...\n")

# Wait for both to complete
p1.wait()
p2.wait()

print("\n✅ BOTH SCRIPTS COMPLETED SUCCESSFULLY!")
