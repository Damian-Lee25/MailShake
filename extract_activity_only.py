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
STATUS_FILE = "activity_migration_status.csv"
LOG_FILE = "activity_extraction.log"

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

# Activity types to extract
ACTIVITY_TYPES = [
    "activity/sent",
    "activity/opens",
    "activity/clicks",
    "activity/replies"
]

# ---------- LOGGING HELPERS ----------
def log(message):
    """Print and write to log file."""
    print(message)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

def log_team_status(team_name, status, error_msg="", campaigns=0, sent=0, opens=0, clicks=0, replies=0):
    """Logs the result of a team's processing to a CSV file."""
    file_exists = os.path.isfile(STATUS_FILE)
    with open(STATUS_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["Timestamp", "Team Name", "Status", "Campaign Count", 
                           "Sent Count", "Opens Count", "Clicks Count", "Replies Count", "Error Message"])
        writer.writerow([
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            team_name, status, campaigns, sent, opens, clicks, replies, error_msg
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

def check_team_has_activity_in_bigquery(client, team_id, activity_table):
    """Check if this team already has activity data in BigQuery."""
    query = f"""
        SELECT COUNT(*) as count
        FROM `{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_bronze']}.{activity_table}`
        WHERE team_id = '{team_id}'
    """
    
    try:
        result = client.query(query).result()
        count = list(result)[0]['count']
        return count > 0, count
    except Exception as e:
        # Table doesn't exist yet or other error
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
                retry_count = 0
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
            
            retry_count = 0
            
            next_token = data.get("nextToken")
            if not next_token:
                break
            payload["nextToken"] = next_token
            
            sleep(1)
            
        except requests.exceptions.ConnectionError as e:
            error_str = str(e)
            if "Failed to resolve" in error_str or "getaddrinfo failed" in error_str:
                retry_count += 1
                log(f"   ⚠️ DNS/Network error for {endpoint} (attempt {retry_count}/{max_retries})")
                if retry_count >= max_retries:
                    log(f"   ❌ Max retries reached due to network issues")
                    break
                wait_time = 30 * retry_count
                log(f"   Waiting {wait_time} seconds before retry...")
                sleep(wait_time)
                continue
            else:
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
    """Return BigQuery schema for activity tables."""
    return [
        bigquery.SchemaField("team_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("message_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("payload", "JSON", mode="REQUIRED"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("source_endpoint", "STRING", mode="REQUIRED"),
    ]

def insert_records(client, table_name, records, team_id, source_endpoint, campaign_id):  # ✅ campaign_id is required
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
            "campaign_id": campaign_id,  # ✅ Use passed parameter instead of payload
            "message_id": str(record.get('messageID')),
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
    """Process a single team's activity extraction and loading."""
    
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
    activity_counts = {"sent": 0, "opens": 0, "clicks": 0, "replies": 0}
    
    try:
        # 1. Fetch campaigns from BigQuery (save API quota!)
        log("\n📧 Fetching campaigns from BigQuery...")
        
        campaigns_query = f"""
            SELECT DISTINCT 
                JSON_VALUE(payload, '$.id') as campaign_id
            FROM `{BIGQUERY_CONFIG['project_id']}.{BIGQUERY_CONFIG['dataset_bronze']}.campaigns`
            WHERE team_id = '{team['team_id']}'
        """
        
        try:
            campaigns_result = client.query(campaigns_query).result()
            campaigns = [{"id": row['campaign_id']} for row in campaigns_result]
            campaign_count = len(campaigns)
            log(f"   Found {campaign_count} campaigns in BigQuery")
        except Exception as e:
            log(f"   ⚠️ Could not fetch campaigns from BigQuery: {e}")
            log(f"   Falling back to API...")
            campaigns = get_mailshake_data("campaigns/list", team["api_key"])
            campaign_count = len(campaigns)
            log(f"   Found {campaign_count} campaigns from API")
        
        if campaign_count == 0:
            log(f"   ℹ️  No campaigns found for {team['team_name']}")
            log_team_status(team['team_name'], "Success", campaigns=0)
            return {"status": "success", "team": team['team_name'], "activities": activity_counts}
        
        # 2. Process each activity type
        for activity_type in ACTIVITY_TYPES:
            activity_name = activity_type.split("/")[1]  # "sent", "opens", "clicks", "replies"
            table_name = activity_type.replace("/", "_")  # "activity_sent", etc.
            
            log(f"\n📊 Processing {activity_name.upper()}...")
            
            # Check if this activity type already has data for this team
            has_data, existing_count = check_team_has_activity_in_bigquery(client, team['team_id'], table_name)
            if has_data:
                log(f"   ℹ️  Team already has {existing_count} {activity_name} records in BigQuery")
                log(f"   ⏩ Skipping {activity_name} extraction (already exists)")
                activity_counts[activity_name] = existing_count
                continue
            
            # Fetch and insert activity PER CAMPAIGN (with campaign_id context)
            log(f"   Fetching and inserting {activity_name} for {campaign_count} campaigns...")
            total_inserted = 0
            all_activity = []  # For JSON backup
            
            for i, campaign in enumerate(campaigns):
                campaign_id = str(campaign.get('id'))
                log(f"      Campaign {i+1}/{campaign_count} (ID: {campaign_id})...")
                
                # Fetch activity for this campaign
                activity_data = get_mailshake_data(activity_type, team["api_key"], 
                                                   params={"campaignID": campaign_id})
                
                log(f"         Found {len(activity_data)} {activity_name} records")
                
                # Insert immediately with campaign_id context
                if len(activity_data) > 0:
                    success = insert_records(client, table_name, activity_data, team['team_id'], 
                                           activity_type, campaign_id)  # ✅ Pass campaign_id
                    if success:
                        total_inserted += len(activity_data)
                        log(f"         Inserted {len(activity_data)} {activity_name} records for campaign {campaign_id}")
                
                # Save for JSON backup
                all_activity.extend(activity_data)
                
                # CRITICAL: Longer pause between campaigns for activity endpoints (10 QU each!)
                if i < len(campaigns) - 1:
                    sleep(35)  # ✅ 35 seconds between campaigns
            
            activity_counts[activity_name] = total_inserted
            log(f"\n   Total {activity_name} collected: {len(all_activity)}")
            log(f"   Total {activity_name} inserted: {total_inserted}")
            
            # Save all activity to JSON backup
            if len(all_activity) > 0:
                save_json(all_activity, f"{team['team_name']}_{table_name}.json")
            
            # CRITICAL: Long cooldown between activity types
            if activity_type != ACTIVITY_TYPES[-1]:  # Not the last one
                cooldown = 60  # 60 seconds between activity types
                log(f"\n⏸️  Cooling down {cooldown} seconds before next activity type...")
                sleep(cooldown)
        
        log(f"\n✅ Team {team['team_name']} processing complete!")
        log(f"   Campaigns: {campaign_count}")
        log(f"   Sent: {activity_counts['sent']}")
        log(f"   Opens: {activity_counts['opens']}")
        log(f"   Clicks: {activity_counts['clicks']}")
        log(f"   Replies: {activity_counts['replies']}")
        
        log_team_status(team['team_name'], "Success", campaigns=campaign_count, 
                       sent=activity_counts['sent'], opens=activity_counts['opens'],
                       clicks=activity_counts['clicks'], replies=activity_counts['replies'])
        
        return {"status": "success", "team": team['team_name'], "activities": activity_counts}
        
    except Exception as e:
        error_msg = str(e)
        log(f"❌ Error processing team {team['team_name']}: {error_msg}")
        log_team_status(team['team_name'], "Failed", error_msg=error_msg, 
                       campaigns=campaign_count, sent=activity_counts['sent'],
                       opens=activity_counts['opens'], clicks=activity_counts['clicks'],
                       replies=activity_counts['replies'])
        return {"status": "failed", "team": team['team_name'], "error": error_msg}

def main():
    """Main execution - sequential processing only."""
    log(f"\n{'='*60}")
    log(f"🚀 Starting Activity-Only Data Extraction")
    log(f"{'='*60}")
    log(f"Total teams to process: {len(TEAMS)}")
    log(f"Activity types: {', '.join([a.split('/')[1] for a in ACTIVITY_TYPES])}")
    log(f"Processing sequentially for maximum reliability...")
    log(f"Status file: {STATUS_FILE}")
    log(f"Log file: {LOG_FILE}\n")
    log(f"⚠️  WARNING: This will be SLOW due to rate limits (10 QU per campaign per activity type)")
    log(f"   Estimated time: Several hours to days depending on campaign counts\n")
    
    results = []
    
    # Sequential processing - one team at a time
    for i, team in enumerate(TEAMS):
        log(f"\n{'#'*60}")
        log(f"Processing team {i+1}/{len(TEAMS)}")
        log(f"{'#'*60}")
        
        result = process_single_team(team)
        results.append(result)
        
        # Very long cooldown between teams for activity extraction
        if i < len(TEAMS) - 1:
            cooldown = 60  # 60 seconds between teams
            log(f"\n⏸️  Cooling down {cooldown} seconds before next team...\n")
            sleep(cooldown)
    
    # Summary report
    log(f"\n{'='*60}")
    log(f"📊 EXECUTION SUMMARY")
    log(f"{'='*60}")
    
    successful = [r for r in results if r["status"] == "success"]
    failed = [r for r in results if r["status"] == "failed"]
    skipped = [r for r in results if r["status"] == "skipped"]
    
    total_sent = sum(r.get("activities", {}).get("sent", 0) for r in successful)
    total_opens = sum(r.get("activities", {}).get("opens", 0) for r in successful)
    total_clicks = sum(r.get("activities", {}).get("clicks", 0) for r in successful)
    total_replies = sum(r.get("activities", {}).get("replies", 0) for r in successful)
    
    log(f"✅ Successful: {len(successful)}/{len(TEAMS)}")
    log(f"⏩ Skipped (already complete): {len(skipped)}/{len(TEAMS)}")
    log(f"❌ Failed: {len(failed)}/{len(TEAMS)}")
    log(f"\n📊 Total activity records extracted:")
    log(f"   Sent: {total_sent:,}")
    log(f"   Opens: {total_opens:,}")
    log(f"   Clicks: {total_clicks:,}")
    log(f"   Replies: {total_replies:,}")
    
    if failed:
        log("\n❌ Failed teams:")
        for f in failed:
            log(f"  - {f['team']}: {f.get('error', 'Unknown error')}")
    
    log(f"\n{'='*60}")
    log(f"✅ Activity extraction complete!")
    log(f"{'='*60}")
    log(f"📄 Check {STATUS_FILE} for detailed status log")
    log(f"📄 Check {LOG_FILE} for full execution log")

if __name__ == "__main__":
    main()