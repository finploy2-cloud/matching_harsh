import requests
import time


BLAST_URL = "https://www.finploy.com/whatsapp_integration/blast_1.php"
def main():
    print("\n🚀 Starting WatiFly Blast Automation...\n")

    while True:
        print("⏳ Triggering PHP blast...")

        try:
            response = requests.get(BLAST_URL)
            text = response.text.strip()
            print("🔥 PHP Blast Response:", text)

            # ❗ SKIP invalid rows, DO NOT stop.
            if "Invalid phone number" in text or "marked invalid" in text:
                print("⚠️ Invalid phone skipped. Checking next candidate...\n")
                time.sleep(2)
                continue

            if "BATCH_COMPLETED" in text:
                print("✔ 100 messages blasted. Moving to next batch...")
                time.sleep(3)
                continue

            if "NO_MORE_CANDIDATES" in text or "NO_PENDING" in text:
                print("🎉 All batches blasted. Stopping...")
                break


        except Exception as e:
            print("❌ Error hitting PHP:", e)

        print("⏳ Waiting 100 sec for next blast...\n")
        time.sleep(100)

    print("\n✔ Blast completed successfully!")

if __name__ == "__main__":
    main()
# ---------------------------------------------------------
# Run not_intrested.py after SQL insertion is done
# ---------------------------------------------------------
try:
    import subprocess
    print("🚀 Running not_intrested.py ...")
    subprocess.run(
        ["python", r"D:\matching_harsh\Job_matching_Screened\candidate_jobs_formate\not_intrested.py"],
        check=True
    )
    print("✔ not_intrested.py executed successfully!")

except Exception as e:
    print(f"❌ Failed to run not_intrested.py: {e}")