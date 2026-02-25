import pandas as pd
import os
import customtkinter as ctk
from tkinter import messagebox

# =====================================================
# Phase 2.2 – Data Cleaning (Final No Tasks 5–7)
# =====================================================

input_file = r"D:\matching_harsh\Lineup_Followup\output\output1.xlsx"
OUTPUT_DIR = r"D:\matching_harsh\Lineup_Followup\output"
output_file = os.path.join(OUTPUT_DIR, "output2.xlsx")

# =====================================================
# Load Excel
# =====================================================
df = pd.read_excel(input_file)
df.columns = [c.strip() for c in df.columns]

# Create lowercase mapping for flexible access
colmap = {c.lower(): c for c in df.columns}

# =====================================================
# 1️⃣ name_location column (always overwrite)
# =====================================================
if "name" in colmap and "location" in colmap:
    df["name_location"] = (
        df[colmap["name"]].fillna("").astype(str).str.strip()
        + "_"
        + df[colmap["location"]].fillna("").astype(str).str.strip()
    )
    df["name_location"] = df["name_location"].str.replace("__", "_").str.strip("_")
else:
    print("⚠️ Missing 'Name' or 'Location' column — cannot generate name_location.")

# =====================================================
# 2️⃣ clean_salary column (FROM curr salary ALWAYS)
# =====================================================

# 🔥 Correct detection (lowercase)
salary_col_key = next((c for c in colmap if "curr" in c and "salary" in c), None)

if salary_col_key:
    salary_col = colmap[salary_col_key]
    salary_series = df[salary_col].astype(str)
else:
    salary_series = pd.Series([""] * len(df))
    print("⚠️ Column 'Curr Salary' not found. Creating empty clean_salary column.")

# 🔥 Remove currency symbols, commas, Lacs text
salary_series = (
    salary_series.str.replace("₹", "", regex=False)
                 .str.replace("$", "", regex=False)
                 .str.replace(",", "", regex=False)
                 .str.replace("Lacs", "", regex=False)
                 .str.replace("lacs", "", regex=False)
                 .str.strip()
)


def safe_float(v):
    """Convert to float safely"""
    try:
        return float(v)
    except:
        return None


def convert_to_lacs(v):
    """Convert raw values to LPA/Lacs format"""
    if v is None:
        return ""
    if v == 0:
        return ""
    # If number is > 1000 → assume rupees → convert to lacs
    if v > 1000:
        return round(v / 100000, 2)
    # If already in lacs
    return round(v, 2)


salary_numeric = salary_series.apply(safe_float)
df["clean_salary"] = salary_numeric.apply(convert_to_lacs)

# =====================================================
# 🟩 GUI – Department & Product Input
# =====================================================

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

        # Department
        ctk.CTkLabel(self, text="Department:").pack(anchor="w", padx=60)
        self.dept_var = ctk.StringVar()
        self.dept_entry = ctk.CTkEntry(self, textvariable=self.dept_var, width=250)
        self.dept_entry.pack(pady=(0, 20))
        self.dept_entry.bind("<Return>", self.focus_next_entry)

        # Product
        ctk.CTkLabel(self, text="Product:").pack(anchor="w", padx=60)
        self.prod_var = ctk.StringVar()
        self.prod_entry = ctk.CTkEntry(self, textvariable=self.prod_var, width=250)
        self.prod_entry.pack(pady=(0, 30))
        self.prod_entry.bind("<Return>", self.trigger_submit)

        # Submit
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

# =====================================================
# 🧩 Apply Department & Product Values
# =====================================================
df["department"] = DEPT_VALUE
df["product"] = PROD_VALUE

print(f"✅ Department set as: {DEPT_VALUE}")
print(f"✅ Product set as: {PROD_VALUE}")

# =====================================================
# 💾 Save Output (no reordering)
# =====================================================
df.to_excel(output_file, index=False)

messagebox.showinfo(
    "Success",
    f"File saved successfully!\n\nDepartment: {DEPT_VALUE}\nProduct: {PROD_VALUE}\n\nSaved as:\n{output_file}"
)

print(f"✅ File saved successfully at: {output_file}")
try:
    import subprocess
    print("▶️ Running main4.py ...")
    subprocess.run(["python", r"D:\matching_harsh\Lineup_Followup\main4.py"], check=True)
    print("✅ main4.py executed successfully!")
except Exception as e:
    print(f"❌ Failed to run main4.py: {e}")