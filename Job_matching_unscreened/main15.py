import pandas as pd
import webbrowser
import customtkinter as ctk
from tkinter import messagebox
from datetime import datetime
import random

# -------------------------------
# File paths and configuration
# -------------------------------
INPUT_FILE = r"D:\matching_harsh\Job_matching_unscreened\output\split_candidate\unique_candidates_2.xlsx"

# -------------------------------
# Load Excel data
# -------------------------------
df = pd.read_excel(INPUT_FILE)

required_cols = ['candidate_id', 'name of candidate', 'link href']
for col in required_cols:
    if col not in df.columns:
        messagebox.showerror("Error", f"Column '{col}' not found in file.")
        raise Exception(f"Column '{col}' not found in Excel.")

df_unique = df[['candidate_id', 'name of candidate', 'link href']].drop_duplicates(subset='candidate_id')
links = df_unique['link href'].dropna().tolist()
names = df_unique['name of candidate'].dropna().tolist()

# Cap at 60 links
max_links = 60
links = links[:max_links]
names = names[:max_links]

# -------------------------------
# Tkinter Temporary Message
# -------------------------------
def show_temp_message(text, duration=3000):
    msg = ctk.CTkToplevel(app)
    msg.geometry("400x150")
    msg.title("Status")
    label = ctk.CTkLabel(msg, text=text, font=ctk.CTkFont(size=14))
    label.pack(expand=True)
    msg.after(duration, msg.destroy)

# -------------------------------
# Tkinter GUI Setup
# -------------------------------
ctk.set_appearance_mode("Light")
ctk.set_default_color_theme("green")

app = ctk.CTk()
app.title("Open Candidate Links")
app.geometry("600x250")

ctk.CTkLabel(
    app, 
    text="Please make sure your Associate App is open.", 
    font=ctk.CTkFont(size=18, weight="bold")
).pack(pady=20)

ctk.CTkLabel(
    app,
    text="Once ready, click 'Proceed' to open candidate links sequentially.",
    font=ctk.CTkFont(size=14)
).pack(pady=10, padx=20)

# -------------------------------
# Async link opener
# -------------------------------
current_index = 0

def open_next_link():
    global current_index
    if current_index >= len(links):
        messagebox.showinfo("Done", f"✅ {len(links)} candidate links opened successfully.")
        app.destroy()
        return

    link = links[current_index]
    name = names[current_index]

    try:
        webbrowser.open_new_tab(link)
        show_temp_message(f"✅ '{name}' profile opened successfully.", duration=3000)
    except Exception as e:
        show_temp_message(f"❌ Failed to open '{name}' profile.", duration=5000)
        return  # Stop processing if failed

    current_index += 1

    # Random delay (5–12 sec) before opening next link
    delay = random.randint(5, 10) * 1000
    app.after(delay, open_next_link)

# -------------------------------
# Proceed button
# -------------------------------
ctk.CTkButton(
    app, 
    text="Proceed", 
    command=open_next_link, 
    width=200, 
    height=50, 
    font=ctk.CTkFont(size=16, weight="bold")
).pack(pady=30)

app.mainloop()
