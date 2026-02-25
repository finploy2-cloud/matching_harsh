# Finploy Job Matching Automation.

This repository contains a set of Python scripts designed to streamline BFSI recruitment workflows. The pipeline processes candidate data, enriches metadata, maps locations, matches candidates to jobs, and prepares ready-to-use datasets for HR operations.

---

## Table of Contents

1. [Candidate Processing & Filtering (`main8.py`)](#1-candidate-processing--filtering-main8py)  
2. [Candidate Data Cleaning & Enrichment (`main9.py`)](#2-candidate-data-cleaning--enrichment-main9py)  
3. [Location ID Mapping (`main10.py`)](#3-location-id-mapping-main10py)  
4. [Final Integration & Enrichment (`main12.py`)](#4-final-integration--enrichment-main12py)  
5. [Google Sheet Master File Downloader (`main13.py`)](#5-google-sheet-master-file-downloader-main13py)  
6. [Job-Candidate Matching (`main14.py`)](#6-job-candidate-matching-main14py)  
7. [Candidate Link Automation (`main15.py`)](#7-candidate-link-automation-main15py)  
8. [Phone Number Integration (`main16.py`)](#8-phone-number-integration-main16py)  
9. [Pipeline Overview](#9-pipeline-overview)  

----
### **Steps To run the Code** :- 
1. Download zip file from Job_matching_automation Repo D:\FINPLOY\Job_Matching_automation--main\Job_Matching_automation--main
2. In job matching Automation folder Create a Folder called final_input , Output , final_output , Screening_Output , Densta_output 
3. Download naukari Data into final_input folder and rename it as input1.xlsx
4. Do this one time only on Terminal (pip install -r requirements.txt) . also there is an additional work on ggoogle cloud console  for which video training is included https://youtu.be/i2KEYHYc1dE
5. Specify the path
4. run  main7.py 
5. run  main9.py
6. run  main10.py
7. run main12.py
8. run main13.py
9. run main14.py
10. run main15.py
11. run main16.py
12. run main17.py
13. run main18.py
7. run  main12.py
---


1 main 7 - path scpecify for 2 links - - 4 outputs phase1_ouput (name & location columns added), screened (name&location col. creation, salary clean), unscreened (name&location col. creation, salary clean), Output1 (unscreened and cleaned for NAUKRI DESIGNATION (e.g sales, reln, field) using ui & ux)
2 main 9 - path scpecify (change date) - Output2 - (DEPT & PRODUCT  in Output 1)  
3 main 10 -  Output 3 - (LOCATION part 1 puttig finploy id locations in output 2) and unmatched locations going into additional_new_location.xlx
4 main 12 - Output 4 - (LOCATION part 2 &  composit key)
5 main 13 (check completeness of mapping before running this)- JOBS - Save Master file (jobs) in final_input
6 MOST IMPORTANT OUTPUT - main 14 - JOB MATCHING - goes in final_ouput as all_job_candidate_matches
7 main 15 - NAUKRI RUN FOR PHONE NUMBERS - links - resdex_phone saved in final_input
8 FINAL OUTPUT WITH PHONE - main 16 - phone number add and create final and save it outside folder - make sure that your file remains closed everytime you run
9 main 17 - Convertng the final output in DINSTAR format 
10 main 18 - to change the filepath to Screenign output 

----


## 1️⃣ Candidate Processing & Filtering (`main7.py`)

**Purpose:** Streamline initial candidate intake, compare with Google Sheets tracker, and allow recruiters to filter candidates via GUI.  

**Features:**

- Reads candidate data from Excel (`input1.xlsx`) and standardizes columns.
- Creates `name_location` (combination of name&location) and splits `employment_detail` into `designation` and `company`.
- Compares with Google Sheets tracker to identify screened vs. unscreened candidates.
- Provides a CustomTkinter GUI for filtering candidates.
- Timestamped outputs with untouched headers highlighted.
- in this output there will be 4 files
- phase1_output..name& lcoation add, cmpany and designation .. for all candidates
- screened_candidates.. already in our scnreening taken out from phase1 and put in here
- unscreened_candidates.. not in our scnreening taken out from phase1 and put in here
- output1_2025... clean up file all steps above from line 27 to line 32.. only for unscreened candidates
- 
**Requirements:**

- Python 3.9+
- Packages: `pandas`, `gspread`, `oauth2client`, `customtkinter`, `openpyxl`
- Google service account JSON (`service_account.json`)
- Input file: `final_input\input1.xlsx`
- Write permissions for `Output\` folder

---

## 2️⃣ Candidate Data Cleaning & Enrichment (`main9.py`)

**Purpose:** Enhance filtered candidate data with clean salary, activity tracking, and department/product assignment.  

**Features:**

- Cleans salary data and converts consistently to Lacs.
- Tracks `Modification` and `Activity`.
- Provides a GUI for assigning `Department` and `Product`. only for unscreened candiates and will be put across all candiates
- Saves standardized output as `output2.xlsx`.

**Dependencies:** `pandas`, `customtkinter`, `tkinter`, `openpyxl`  

---

## 3️⃣ Location ID Mapping (`main10.py`)

**Purpose:** Assign unique `finploy_id` to candidates based on location using a reference Google Sheet.  

**Features:**

- Maps locations to IDs (city → area → unmatched).
- Tracks unmatched locations in `additional_new_location.xlsx`.
- Saves updated dataset as `output3.xlsx`.

**Dependencies:** `pandas`, `gspread`, `oauth2client`, `openpyxl`, `re`, `os`

---

## 4️⃣ Final Integration & Enrichment (`main12.py`)

**Purpose:** Consolidates previous outputs, maps location metadata, assigns candidate IDs, and generates composite keys.  

In case if we prefer to update the location file in google drive then filepath is Drive new_location
https://docs.google.com/spreadsheets/d/11Yye2zMLOgb0J8wBjH0VJNuOV28AAERNPxr3RE2OO-E/edit?gid=1617031420#gid=1617031420

**Features:**

- Maps location metadata: `finploy_id`, `area`, `city`, `state`, `city_id`, `candidate_pincode`.
- Populates `department` and `product`.
- Creates `composit_key = city_id_product_department_clean_salary`.
- Adds sequential `candidate_id`.
- Saves standardized output as `output4.xlsx`.

**Dependencies:** `pandas`, `gspread`, `oauth2client`, `openpyxl`, `os`, `string`

---

## 5️⃣ Google Sheet Master File Downloader (`main13.py`)

**Purpose:** Downloads a Google Sheet as Excel for master input.  

**Features:**

- Authenticates via service account.
- Downloads Google Sheet in XLSX format using Drive API.
- Saves as `final_input\{date}_MASTER FILE LOCATIONS.xlsx`.

**Dependencies:** `google-auth`, `google-auth-oauthlib`, `google-auth-httplib2`, `google-api-python-client`, `io`, `datetime`, `os`

---

## 6️⃣ Job-Candidate Matching (`main14.py`)

**Purpose:** Match candidates to jobs based on composite keys and salary thresholds.  

**Features:**

- Loads master job and candidate Excel files.
- Validates composite keys.
- Matches candidates by prefix and salary ≤ job salary.
- Exports combined Excel `all_job_candidate_matches.xlsx` with job details.

**Dependencies:** `pandas`, `openpyxl`, `os`, `getpass` (optional)

---

## 7️⃣ Candidate Link Automation (`main15.py`)

**Purpose:** Open candidate profile links in browser and upload to Google Sheet.  

**Features:**

- Removes duplicate links.
- Uploads candidate data to Google Sheet with date.
- Opens links in browser tabs automatically.
- CustomTkinter GUI for simple interaction.

**Dependencies:** `pandas`, `customtkinter`, `tkinter`, `webbrowser`, `gspread`, `oauth2client`, `datetime`

---

## 8️⃣ Phone Number Integration (`main16.py`)

**Purpose:** Merge cleaned phone numbers into candidate-job dataset.  

**Features:**

- Loads phone numbers and candidate-job match files.
- Keeps unique `name_location → clean_phone` mapping.
- Left merges into main dataset.
- Reorders `clean_phone` column to 5th position.
- Saves final output as `all_job_candidate_matches_with_phone.xlsx`.

**Dependencies:** `pandas`

---

## 9️⃣ Pipeline Overview

| Phase | Script | Input | Output | Description |
|-------|--------|-------|--------|-------------|
| 1 | `main8.py` | `input1.xlsx` | `output1.xlsx` | Candidate intake and filtering |
| 2 | `main9.py` | `output1.xlsx` | `output2.xlsx` | Clean, enrich, track activity |
| 3 | `main10.py` | `output2.xlsx` | `output3.xlsx` | Map location IDs |
| 4 | `main12.py` | `output3.xlsx` | `output4.xlsx` | Final integration & composite key |
| 5 | `main13.py` | Google Sheet | `{date}_MASTER FILE LOCATIONS.xlsx` | Download master jobs/locations |
| 6 | `main14.py` | Jobs + Candidates | `all_job_candidate_matches.xlsx` | Match candidates to jobs |
| 7 | `main15.py` | `all_job_candidate_matches.xlsx` | Google Sheet | Open candidate links and upload |
| 8 | `main16.py` | Phone Excel + matches | `all_job_candidate_matches_with_phone.xlsx` | Merge phone numbers |

---

### **Usage**

1. Ensure all dependencies are installed and service account JSON is available.  
2. Run scripts sequentially: `main8.py → main16.py`.  
3. Check output files in the designated `Output` folders.  
4. Follow GUI prompts where applicable.  

---

### **Notes**

- All outputs are timestamped for tracking.  
- Duplicate handling ensures no candidate data is lost.  
- Google Sheets integration requires write permissions.  
- Scripts can be modified for additional columns or custom logic.  


