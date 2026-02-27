import requests
import json
import csv
import os
from pathlib import Path
from time import sleep
from datetime import datetime, timezone
from google.cloud import bigquery
from dotenv import load_dotenv

# ---------- CONFIGURATION ----------
RAW_DATA_DIR = Path("raw_mailshake_data")
RAW_DATA_DIR.mkdir(exist_ok=True)
STATUS_FILE = "recipients_migration_status.csv"
LOG_FILE = "recipients_extraction.log"

load_dotenv()

BIGQUERY_CONFIG = {
    "project_id": os.getenv("BIGQUERY_PROJECT_ID"),
    "dataset_bronze": "BRONZE",
    "credentials_path": os.getenv("BIGQUERY_CREDENTIALS")
}

with open("teams_config.json") as f:
    teams_data = json.load(f)

TEAMS = [
    {"team_id": team["teamID"], "api_key": team["apiKey"], "team_name": team["teamName"]}
    for team in teams_data["teams"]
]

BASE_URL = "https://api.mailshake.com/2017-04-01"

# ---------- LOGGING HELPERS ----------
def log(message):
    """Print and write to log file."""
    print(message)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

def log_team_status(team_name, status, error_msg="", campaigns=0, recipients=0):
    """Logs the result of a team's processing to a CSV file."""
    file_exists = os.path.isfile(STATUS_FILE)
    with open(STATUS_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Timestamp", "Team Name", "Status", "Campaign Count", "Recipient Count", "Error Message"])
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            team_name, status, campaigns, recipients, error_msg
        ])

def is_team_complete(team_name):
    """Checks the CSV to see if this team was already successfully processed."""
    if not os.path.isfile(STATUS_FILE):
        return False
    with open(STATUS_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        return any(row and len(row) > 1 and row[1] == team_name and row[2] == "Success" for row in reader)

# ---------- BIGQUERY HELPERS ----------
def get_bigquery_client():
    """Create BigQuery client."""
    return bigquery.Client.from_service_account_json(
        BIGQUERY_CONFIG["credentials_path"],
        project=BIGQUERY_CONFIG["project_id"]
    )

def check_team_has_recipients_in_bigquery(client, team_id):
    """Check if this team already has recipients data in BigQuery."""
    query = f"""
        SELECT COUNT(*) as count
        FROM `{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_bronze']}.recipients`
        WHERE team_id = '{team_id}'
    """
    
    try:
        result = client.query(query).result()
        count = list(result)[0]['count']
        return count > 0, count
    except Exception as e:
        # Table doesn't exist yet or other error
        log(f"   ℹ️  Could not check existing data: {e}")
        return False, 0

def check_internet_connection():
    """Check if internet connection is available."""
    try:
        requests.get("https://www.google.com", timeout=5)
        return True
    except:
        return False

# ---------- API HELPERS ----------
def get_mailshake_data(endpoint, api_key, params=None):
    """Fetch data from Mailshake API with pagination and rate limit handling."""
    all_results = []
    url = f"{BASE_URL}/{endpoint}"
    payload = {"apiKey": api_key}
    if params:
        payload.update(params)
    
    retry_count = 0
    max_retries = 5
    
    while True:
        try:
            response = requests.post(url, data=payload, timeout=30)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                log(f"   ⚠️ Rate limit hit for {endpoint}. Waiting {retry_after} seconds...")
                sleep(retry_after)
                retry_count = 0  # Reset retry count after rate limit
                continue
            
            if response.status_code == 502:
                log(f"   ⚠️ 502 Bad Gateway - API overloaded. Waiting 30s...")
                sleep(30)
                retry_count += 1
                if retry_count >= max_retries:
                    log(f"   ❌ Max retries reached for {endpoint}")
                    break
                continue

            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            all_results.extend(results)
            
            # Reset retry count on successful request
            retry_count = 0
            
            next_token = data.get("nextToken")
            if not next_token:
                break
            payload["nextToken"] = next_token
            
            sleep(1)  # 1 second delay between pagination calls
            
        except requests.exceptions.ConnectionError as e:
            # DNS or network connection errors
            error_str = str(e)
            if "Failed to resolve" in error_str or "getaddrinfo failed" in error_str:
                retry_count += 1
                log(f"   ⚠️ DNS/Network error for {endpoint} (attempt {retry_count}/{max_retries})")
                if retry_count >= max_retries:
                    log(f"   ❌ Max retries reached due to network issues")
                    break
                # Exponential backoff for DNS issues
                wait_time = 30 * retry_count  # 30s, 60s, 90s, 120s, 150s
                log(f"   Waiting {wait_time} seconds before retry...")
                sleep(wait_time)
                continue
            else:
                # Other connection errors
                retry_count += 1
                log(f"   ❌ Connection error for {endpoint}: {e}")
                if retry_count >= max_retries:
                    break
                log(f"   Retrying in 15 seconds... ({retry_count}/{max_retries})")
                sleep(15)
                continue
                
        except requests.exceptions.RequestException as e:
            log(f"   ❌ Error fetching {endpoint}: {e}")
            retry_count += 1
            if retry_count >= max_retries:
                log(f"   ❌ Max retries reached")
                break
            log(f"   Retrying in 10 seconds... ({retry_count}/{max_retries})")
            sleep(10)
    
    return all_results

def save_json(data, filename):
    """Save data to JSON file for backup."""
    path = RAW_DATA_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    log(f"   💾 Saved {filename} ({len(data)} records)")

def get_table_schema(table_name):
    """Return BigQuery schema for recipients table."""
    return [
        bigquery.SchemaField("team_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("recipient_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("source_endpoint", "STRING", mode="REQUIRED"),
    ]

def insert_records(client, table_name, records, team_id, source_endpoint, campaign_id):  # ✅ CHANGED - campaign_id is now required
    """Insert records into BigQuery table in batches."""
    if not records:
        log(f"   ⚠️  No data to load for {table_name}")
        return False
    
    dataset_id = f"{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_bronze']}"
    table_id = f"{dataset_id}.{table_name}"
    
    # Get schema
    schema = get_table_schema(table_name)
    
    # Create table if it doesn't exist
    table = bigquery.Table(table_id, schema=schema)
    try:
        client.create_table(table, exists_ok=True)
        log(f"   ✅ Table {table_name} ready")
    except Exception as e:
        log(f"   ❌ ERROR creating table: {e}")
        return False
    
    # Prepare rows
    extracted_at = datetime.now(timezone.utc).isoformat()
    rows_to_insert = []
    
    for record in records:
        row = {
            "team_id": team_id,
            "campaign_id": campaign_id,  # ✅ CHANGED - Use passed campaign_id instead of payload
            "recipient_id": str(record.get('id')),
            "payload": json.dumps(record),
            "extracted_at": extracted_at,
            "source_endpoint": source_endpoint
        }
        rows_to_insert.append(row)
    
    # Insert into BigQuery in batches
    BATCH_SIZE = 1000
    total_rows = len(rows_to_insert)
    total_inserted = 0
    
    log(f"   Inserting {total_rows} rows in batches of {BATCH_SIZE}...")
    
    for i in range(0, total_rows, BATCH_SIZE):
        batch = rows_to_insert[i:i + BATCH_SIZE]
        batch_num = (i // BATCH_SIZE) + 1
        total_batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
        
        try:
            errors = client.insert_rows_json(table_id, batch)
            
            if errors:
                log(f"   ❌ Batch {batch_num}/{total_batches} errors: {errors[:2]}")
            else:
                total_inserted += len(batch)
                log(f"   ✅ Batch {batch_num}/{total_batches} - Inserted {len(batch)} rows (Total: {total_inserted}/{total_rows})")
            
            if i + BATCH_SIZE < total_rows:
                sleep(2)
                
        except Exception as e:
            log(f"   ❌ Exception in batch {batch_num}/{total_batches}: {e}")
            continue
    
    if total_inserted == total_rows:
        log(f"   ✅ Successfully loaded all {total_rows} records into {table_name}")
        return True
    elif total_inserted > 0:
        log(f"   ⚠️  Partially loaded {total_inserted}/{total_rows} records into {table_name}")
        return True
    else:
        log(f"   ❌ Failed to load any records into {table_name}")
        return False

# ---------- MAIN PROCESS ----------
def process_single_team(team):
    """Process a single team's recipients extraction and loading."""
    
    # Check internet before starting
    if not check_internet_connection():
        log(f"⚠️ No internet connection detected. Waiting 30 seconds...")
        sleep(30)
        if not check_internet_connection():
            log(f"❌ Still no internet. Skipping {team['team_name']}")
            log_team_status(team['team_name'], "Failed", error_msg="No internet connection")
            return {"status": "failed", "team": team['team_name'], "error": "No internet"}
    
    # Check if team is already complete in status file
    if is_team_complete(team['team_name']):
        log(f"⏩ Skipping {team['team_name']} (Already marked Success in CSV)")
        return {"status": "skipped", "team": team['team_name']}

    log(f"\n{'='*60}")
    log(f"📊 Processing team: {team['team_name']} (ID: {team['team_id']})")
    log(f"{'='*60}")
    
    client = get_bigquery_client()
    campaign_count = 0
    total_recipients_inserted = 0  # ✅ Track across all campaigns
    all_recipients = []  # ✅ For JSON backup
    
    # Check if team already has data in BigQuery
    has_data, existing_count = check_team_has_recipients_in_bigquery(client, team['team_id'])
    if has_data:
        log(f"   ℹ️  Team already has {existing_count} recipients in BigQuery")
        log(f"   ⏩ Skipping data extraction (already exists)")
        log_team_status(team['team_name'], "Success", campaigns=0, recipients=existing_count)
        return {"status": "skipped", "team": team['team_name'], "recipients": existing_count, "reason": "already_in_bigquery"}
    
    try:
        # 1. Fetch campaigns (need this to get campaign IDs)
        log("\n📧 Fetching campaigns...")
        campaigns = get_mailshake_data("campaigns/list", team["api_key"])
        campaign_count = len(campaigns)
        log(f"   Found {campaign_count} campaigns")
        
        if campaign_count == 0:
            log(f"   ℹ️  No campaigns found for {team['team_name']}")
            log_team_status(team['team_name'], "Success", campaigns=0, recipients=0)
            return {"status": "success", "team": team['team_name'], "recipients": 0}
        
        # 2. Fetch and insert recipients PER CAMPAIGN (with campaign_id context)
        log(f"\n📬 Fetching and inserting recipients for {campaign_count} campaigns...")
        
        for i, campaign in enumerate(campaigns):
            campaign_id = str(campaign.get('id'))  # ✅ Convert to string
            log(f"   Campaign {i+1}/{campaign_count} (ID: {campaign_id})...")
            
            # Fetch recipients for this campaign
            recipients = get_mailshake_data("recipients/list", team["api_key"], 
                                           params={"campaignID": campaign_id})
            
            log(f"      Found {len(recipients)} recipients")
            
            # Insert immediately with campaign_id context
            if len(recipients) > 0:
                success = insert_records(client, "recipients", recipients, team['team_id'], 
                                       "recipients/list", campaign_id)  # ✅ Pass campaign_id
                if success:
                    total_recipients_inserted += len(recipients)
                    log(f"      Inserted {len(recipients)} recipients for campaign {campaign_id}")
            
            # Save for JSON backup
            all_recipients.extend(recipients)
            
            # Pause between campaigns (rate limit protection)
            if i < len(campaigns) - 1:
                sleep(30)  # ✅ 30 seconds between campaigns (increased from 25)
        
        log(f"\n   Total recipients collected: {len(all_recipients)}")
        log(f"   Total recipients inserted: {total_recipients_inserted}")
        
        # Save all recipients to JSON backup
        if len(all_recipients) > 0:
            save_json(all_recipients, f"{team['team_name']}_recipients.json")
        
        # Check success
        if total_recipients_inserted == len(all_recipients):
            log(f"\n✅ Team {team['team_name']} processing complete!")
            log(f"   Campaigns: {campaign_count}")
            log(f"   Recipients: {total_recipients_inserted}")
            log_team_status(team['team_name'], "Success", campaigns=campaign_count, recipients=total_recipients_inserted)
            return {"status": "success", "team": team['team_name'], "recipients": total_recipients_inserted}
        elif total_recipients_inserted > 0:
            log(f"\n⚠️  Team {team['team_name']} partially complete")
            log(f"   Campaigns: {campaign_count}")
            log(f"   Recipients inserted: {total_recipients_inserted}/{len(all_recipients)}")
            log_team_status(team['team_name'], "Success", campaigns=campaign_count, recipients=total_recipients_inserted)
            return {"status": "success", "team": team['team_name'], "recipients": total_recipients_inserted}
        else:
            raise Exception("Failed to insert any recipients into BigQuery")
        
    except Exception as e:
        error_msg = str(e)
        log(f"❌ Error processing team {team['team_name']}: {error_msg}")
        log_team_status(team['team_name'], "Failed", error_msg=error_msg, 
                       campaigns=campaign_count, recipients=total_recipients_inserted)
        return {"status": "failed", "team": team['team_name'], "error": error_msg}

def main():
    """Main execution - sequential processing only."""
    log(f"\n{'='*60}")
    log(f"🚀 Starting Recipients-Only Data Extraction")
    log(f"{'='*60}")
    log(f"Total teams to process: {len(TEAMS)}")
    log(f"Processing sequentially for maximum reliability...")
    log(f"Status file: {STATUS_FILE}")
    log(f"Log file: {LOG_FILE}\n")
    
    results = []
    
    # Sequential processing - one team at a time
    for i, team in enumerate(TEAMS):
        log(f"\n{'#'*60}")
        log(f"Processing team {i+1}/{len(TEAMS)}")
        log(f"{'#'*60}")
        
        result = process_single_team(team)
        results.append(result)
        
        # Cooldown between teams
        if i < len(TEAMS) - 1:
            cooldown = 45  # ✅ 45 seconds between teams (increased from 30 for safety)
            log(f"\n⏸️  Cooling down {cooldown} seconds before next team...\n")
            sleep(cooldown)
    
    # Summary report
    log(f"\n{'='*60}")
    log(f"📊 EXECUTION SUMMARY")
    log(f"{'='*60}")
    
    successful = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "failed"]
    skipped = [r for r in results if r["status"] == "skipped"]
    
    total_recipients = sum(r.get("recipients", 0) for r in successful)
    
    log(f"✅ Successful: {len(successful)}/{len(TEAMS)}")
    log(f"⏩ Skipped (already complete): {len(skipped)}/{len(TEAMS)}")
    log(f"❌ Failed: {len(failed)}/{len(TEAMS)}")
    log(f"📊 Total recipients extracted: {total_recipients:,}")
    
    if failed:
        log("\n❌ Failed teams:")
        for f in failed:
            log(f"  - {f['team']}: {f.get('error', 'Unknown error')}")
    
    log(f"\n{'='*60}")
    log(f"✅ Recipients extraction complete!")
    log(f"{'='*60}")
    log(f"📄 Check {STATUS_FILE} for detailed status log")
    log(f"📄 Check {LOG_FILE} for full execution log")

if __name__ == "__main__":
    main()