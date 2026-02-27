import requests
import json
import csv
import os
from pathlib import Path
from time import sleep
from datetime import datetime, timezone
from google.cloud import bigquery  
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# ---------- CONFIGURATION ----------
RAW_DATA_DIR = Path("raw_mailshake_data")
RAW_DATA_DIR.mkdir(exist_ok=True)
STATUS_FILE = "migration_status.csv"

load_dotenv()

BIGQUERY_CONFIG = {
    "project_id": os.getenv("BIGQUERY_PROJECT_ID"),
    "dataset_bronze": "BRONZE",
    "dataset_silver": "SILVER", 
    "dataset_gold": "GOLD",
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
def log_team_status(team_name, status, error_msg="", campaigns=0):
    """Logs the result of a team's processing to a CSV file."""
    file_exists = os.path.isfile(STATUS_FILE)
    with open(STATUS_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Timestamp", "Team Name", "Status", "Campaign Count", "Error Message"])
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            team_name, status, campaigns, error_msg
        ])

def is_team_complete(team_name):
    """Checks the CSV to see if this team was already successfully processed."""
    if not os.path.isfile(STATUS_FILE):
        return False
    with open(STATUS_FILE, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        return any(row and row[1] == team_name and row[2] == "Success" for row in reader)

# ---------- HELPER FUNCTIONS ----------
def get_mailshake_data(endpoint, api_key, params=None):
    """Fetch data from Mailshake API with pagination and rate limit handling."""
    all_results = []
    url = f"{BASE_URL}/{endpoint}"
    payload = {"apiKey": api_key}
    if params:
        payload.update(params)
    
    while True:
        try:
            response = requests.post(url, data=payload, timeout=30)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                print(f"⚠️ Rate limit hit for {endpoint}. Waiting {retry_after} seconds...")
                sleep(retry_after)
                continue

            response.raise_for_status()
            data = response.json()
            all_results.extend(data.get("results", []))
            
            next_token = data.get("nextToken")
            if not next_token:
                break
            payload["nextToken"] = next_token
            
            sleep(0.5)  # Small delay between pagination calls
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Error fetching {endpoint}: {e}")
            break
    
    return all_results

def save_json(data, filename):
    """Save data to JSON file for backup."""
    path = RAW_DATA_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"💾 Saved {filename} ({len(data)} records)")

def get_bigquery_client():
    """Create BigQuery client."""
    return bigquery.Client.from_service_account_json(
        BIGQUERY_CONFIG["credentials_path"],
        project=BIGQUERY_CONFIG["project_id"]
    )
def get_table_schema(table_name):
    """Return BigQuery schema for a given table."""
    base_schema = [
        bigquery.SchemaField("team_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("source_endpoint", "STRING", mode="REQUIRED"),
    ]
    
    # Add table-specific ID fields at the beginning (after team_id)
    if table_name == "campaigns":
        base_schema.insert(1, bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"))
    elif table_name == "recipients":
        base_schema.insert(1, bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"))
        base_schema.insert(2, bigquery.SchemaField("recipient_id", "STRING", mode="REQUIRED"))
    elif table_name == "leads":
        base_schema.insert(1, bigquery.SchemaField("lead_id", "STRING", mode="REQUIRED"))
    elif table_name == "senders":
        base_schema.insert(1, bigquery.SchemaField("sender_id", "STRING", mode="REQUIRED"))
    elif table_name == "team_members":
        base_schema.insert(1, bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"))
    elif table_name in ["activity_sent", "activity_opens", "activity_replies"]:
        base_schema.insert(1, bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"))
        base_schema.insert(2, bigquery.SchemaField("message_id", "STRING", mode="REQUIRED"))
    elif table_name == "lead_status_changes":
        base_schema.insert(1, bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"))
        base_schema.insert(2, bigquery.SchemaField("lead_id", "STRING", mode="REQUIRED"))
    
    return base_schema

def insert_records(client, table_name, records, team_id, source_endpoint):
    """Insert records into BigQuery table."""
    if not records:
        print(f"⚠️  No data to load for {table_name}")
        return

    # ========== NEW: CREATE TABLE IF IT DOESN'T EXIST ==========
    dataset_id = f"{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_bronze']}"
    table_id = f"{dataset_id}.{table_name}"
    
    # Get schema for this table
    schema = get_table_schema(table_name)
    
    # Create table if it doesn't exist
    table = bigquery.Table(table_id, schema=schema)
    try:
        client.create_table(table, exists_ok=True)
        print(f"✅ Table {table_name} ready")
    except Exception as e:
        print(f"⚠️  Error with table {table_name}: {e}")
    # ========== END NEW CODE ==========
    
    extracted_at = datetime.now(timezone.utc).isoformat()
    
    # Prepare rows for BigQuery
    rows_to_insert = []
    for record in records:
        row = {
            "team_id": team_id,
            "payload": json.dumps(record),
            "extracted_at": extracted_at,
            "source_endpoint": source_endpoint
        }
        
        # Add table-specific fields
        if table_name == "campaigns":
            row["campaign_id"] = str(record.get('id'))
            
        elif table_name == "recipients":
            row["campaign_id"] = str(record.get('campaignID'))
            row["recipient_id"] = str(record.get('id'))
            
        elif table_name == "leads":
            row["lead_id"] = str(record.get('id'))
            
        elif table_name == "senders":
            row["sender_id"] = str(record.get('id'))
            
        elif table_name == "team_members":
            row["user_id"] = str(record.get('id'))
            
        elif table_name in ["activity_sent", "activity_opens", "activity_replies"]:
            row["campaign_id"] = str(record.get('campaignID'))
            row["message_id"] = str(record.get('messageID'))
            
        elif table_name == "lead_status_changes":
            row["campaign_id"] = str(record.get('campaignID'))
            row["lead_id"] = str(record.get('leadID'))
        
        rows_to_insert.append(row)
    
    # Insert into BigQuery
    table_id = f"{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_bronze']}.{table_name}"
    errors = client.insert_rows_json(table_id, rows_to_insert)
    
    if errors:
        print(f"❌ Errors inserting into {table_name}: {errors}")
    else:
        print(f"✅ Loaded {len(records)} records into {table_name}")

# ---------- MAIN PROCESS ----------
def process_single_team(team):
    """Process a single team's data extraction and loading."""
    
    # Check if team is already complete
    if is_team_complete(team['team_name']):
        print(f"⏩ Skipping {team['team_name']} (Already marked Success in CSV)")
        return {"status": "skipped", "team": team['team_name']}

    print(f"\n{'='*60}\n📊 Processing team: {team['team_name']} (ID: {team['team_id']})\n{'='*60}")
    
    client = get_bigquery_client()
    campaign_count = 0
    
    try:
        # 1. Campaigns (1-2 QU per call)
        print("\n📧 Fetching campaigns...")
        campaigns = get_mailshake_data("campaigns/list", team["api_key"])
        campaign_count = len(campaigns)
        save_json(campaigns, f"{team['team_name']}_campaigns.json")
        insert_records(client, "campaigns", campaigns, team['team_id'], "campaigns/list")
        
        # 2. Leads (1-2 QU per call)
        print("\n👤 Fetching leads...")
        leads = get_mailshake_data("leads/list", team["api_key"])
        save_json(leads, f"{team['team_name']}_leads.json")
        insert_records(client, "leads", leads, team['team_id'], "leads/list")
        
        # 3. Recipients (10 QU per campaign - HIGH COST)
        print("\n📬 Fetching recipients...")
        all_recipients = []
        for i, campaign in enumerate(campaigns):
            campaign_id = campaign.get('id')
            print(f"  Processing campaign {i+1}/{len(campaigns)} (ID: {campaign_id})...")
            res = get_mailshake_data("recipients/list", team["api_key"], params={"campaignID": campaign_id})
            all_recipients.extend(res)
            if i < len(campaigns) - 1:
                sleep(10.0)  # 10 second pause between campaigns
        save_json(all_recipients, f"{team['team_name']}_recipients.json")
        
        
        # 4. Senders (1-2 QU per call)
        print("\n✉️ Fetching senders...")
        senders = get_mailshake_data("senders/list", team["api_key"])
        save_json(senders, f"{team['team_name']}_senders.json")
        insert_records(client, "senders", senders, team['team_id'], "senders/list")
        
        # 5. Team Members (1-2 QU per call)
        print("\n👥 Fetching team members...")
        team_members = get_mailshake_data("team/list-members", team["api_key"])
        save_json(team_members, f"{team['team_name']}_team_members.json")
        insert_records(client, "team_members", team_members, team['team_id'], "team/list-members")
        
        # COOLDOWN: Let quota window reset before hitting high-cost activity endpoints
        print("\n⏸️  Cooling down for 60 seconds before activity endpoints...")
        sleep(60)
        
        # 6. Activity (10 QU per campaign × 3 types - VERY HIGH COST)
        print("\n📊 Fetching activity...")

        # 7. Lead Status Changes (1-2 QU per call)
        print("\n🔄 Fetching lead status changes...")
        status_changes = get_mailshake_data("leads/list-status-changes", team["api_key"])
        save_json(status_changes, f"{team['team_name']}_lead_status_changes.json")
        insert_records(client, "lead_status_changes", status_changes, team['team_id'], "leads/list-status-changes")
        
        
        print(f"\n✅ Team {team['team_name']} processing complete!")
        log_team_status(team['team_name'], "Success", campaigns=campaign_count)
        return {"status": "success", "team": team['team_name']}
        
    except Exception as e:
        print(f"❌ Error processing team {team['team_name']}: {e}")
        
        log_team_status(team['team_name'], "Failed", error_msg=str(e), campaigns=campaign_count)
        return {"status": "failed", "team": team['team_name'], "error": str(e)}
    
    

def main():
    """Main execution with sequential processing and progress tracking."""
    print(f"\n{'='*60}\n🚀 Starting Mailshake Data Extraction\n{'='*60}")
    print(f"Total teams to process: {len(TEAMS)}")
    
    # Sequential processing (MAX_WORKERS=1) for rate limit stability
    MAX_WORKERS = 3
    print(f"Processing teams sequentially for rate limit stability...\n")
    
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_team = {executor.submit(process_single_team, team): team for team in TEAMS}
        
        for future in as_completed(future_to_team):
            team = future_to_team[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"❌ Unexpected error for team {team['team_name']}: {e}")
                results.append({"status": "failed", "team": team['team_name'], "error": str(e)})
    
    # Summary report
    print(f"\n{'='*60}\n📊 EXECUTION SUMMARY\n{'='*60}")
    successful = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "failed"]
    skipped = [r for r in results if r["status"] == "skipped"]
    
    print(f"✅ Successful: {len(successful)}/{len(TEAMS)}")
    print(f"⏩ Skipped (already complete): {len(skipped)}/{len(TEAMS)}")
    print(f"❌ Failed: {len(failed)}/{len(TEAMS)}")
    
    if failed:
        print("\n❌ Failed teams:")
        for f in failed:
            print(f"  - {f['team']}: {f.get('error', 'Unknown error')}")
    
    print(f"\n{'='*60}\n✅ Data extraction complete!\n{'='*60}")
    print(f"📄 Check {STATUS_FILE} for detailed status log")

if __name__ == "__main__":
    main()