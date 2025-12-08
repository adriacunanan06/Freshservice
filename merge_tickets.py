import threading
import time
import json
import logging
import os
import datetime
from collections import defaultdict
from flask import Flask
import requests

# ================= CONFIGURATION =================
DOMAIN = os.environ.get("FRESHDESK_DOMAIN")
API_KEY = os.environ.get("FRESHDESK_API_KEY")

# Set to False to ACTUALLY merge tickets
DRY_RUN = True  

CHECKPOINT_FILE = "merge_checkpoint.json"
LOG_FILE = "merge_tickets.log"
# =================================================

app = Flask(__name__)
BASE_URL = f"https://{DOMAIN}/api/v2"
AUTH = (API_KEY, "X")
HEADERS = {"Content-Type": "application/json"}

# --- SETUP LOGGING (File + Console) ---
# This saves logs to 'merge_tickets.log' AND prints to the screen
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def log(msg):
    logging.info(msg)

def format_time(seconds):
    return str(datetime.timedelta(seconds=int(seconds)))

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                data = json.load(f)
            return set(data.get('processed_requesters', []))
        except Exception as e:
            log(f"Error loading checkpoint: {e}")
    return set()

def save_checkpoint(processed_set):
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({"processed_requesters": list(processed_set)}, f)
    except Exception as e:
        log(f"Error saving checkpoint: {e}")

def check_rate_limit(response):
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        log(f"⚠️ Rate limit hit. Sleeping for {retry_after} seconds...")
        time.sleep(retry_after + 5)
        return True
    return False

def get_all_tickets():
    all_tickets = []
    page = 1
    per_page = 100
    log("📥 Fetching tickets from Freshdesk...")
    
    while True:
        url = f"{BASE_URL}/tickets?per_page={per_page}&page={page}"
        try:
            response = requests.get(url, auth=AUTH)
            if check_rate_limit(response): continue
            
            if response.status_code != 200:
                log(f"❌ Error fetching page {page}: {response.text}")
                break
                
            data = response.json()
            if not data: break
            
            all_tickets.extend(data)
            if page % 5 == 0:
                log(f"   Fetched page {page}... Total so far: {len(all_tickets)}")
            page += 1
            
        except Exception as e:
            log(f"❌ Network error: {e}")
            time.sleep(5)
            
    return all_tickets

def merge_tickets(primary_id, secondary_ids):
    url = f"{BASE_URL}/tickets/{primary_id}/merge"
    payload = { "secondary_ticket_ids": secondary_ids }
    
    if DRY_RUN:
        # log(f"   [DRY RUN] Would merge {len(secondary_ids)} tickets") # Reduced noise
        return True
            
    try:
        response = requests.put(url, auth=AUTH, headers=HEADERS, data=json.dumps(payload))
        if check_rate_limit(response): return merge_tickets(primary_id, secondary_ids)
            
        if response.status_code in [200, 204]:
            log(f"✅ Merged {len(secondary_ids)} into #{primary_id}")
            return True
        else:
            log(f"❌ FAILED merge #{primary_id}: {response.text}")
            return False
    except Exception as e:
        log(f"❌ Error merging: {e}")
        return False

def run_merge_process():
    log("========================================")
    log("STARTING TICKET MERGE PROCESS")
    log(f"Mode: {'DRY RUN (Safe)' if DRY_RUN else 'LIVE (Destructive)'}")
    log("========================================")

    processed_requesters = load_checkpoint()
    
    # 1. Fetch
    tickets = get_all_tickets()
    total_tickets = len(tickets)
    log(f"📦 Total tickets: {total_tickets}")
    
    # 2. Group
    log("🔍 Grouping tickets...")
    tickets_by_requester = defaultdict(list)
    for t in tickets:
        tickets_by_requester[t['requester_id']].append({
            'id': t['id'],
            'created_at': t['created_at']
        })
    
    # 3. Prepare Work List
    work_list = []
    for requester_id, user_tickets in tickets_by_requester.items():
        # Skip if done or not enough tickets
        if str(requester_id) in processed_requesters or requester_id in processed_requesters:
            continue
        if len(user_tickets) < 2:
            processed_requesters.add(requester_id)
            continue
        work_list.append((requester_id, user_tickets))

    total_groups = len(work_list)
    log(f"🚀 Found {total_groups} groups to process.")
    
    # 4. Process with ETA
    start_time = time.time()
    processed_count = 0
    
    for i, (requester_id, user_tickets) in enumerate(work_list):
        
        # Sort & Identify
        user_tickets.sort(key=lambda x: x['created_at'])
        primary = user_tickets[-1] 
        secondary_tickets = user_tickets[:-1]
        secondary_ids = [t['id'] for t in secondary_tickets]
        
        # Perform Merge
        success = merge_tickets(primary['id'], secondary_ids)
        
        if success:
            processed_requesters.add(requester_id)
            save_checkpoint(processed_requesters)
        
        processed_count += 1
        
        # --- ETA CALCULATION ---
        if processed_count % 5 == 0: # Update stats every 5 groups
            elapsed = time.time() - start_time
            avg_time = elapsed / processed_count
            remaining = total_groups - processed_count
            eta_seconds = remaining * avg_time
            
            percent = (processed_count / total_groups) * 100
            log(f"⏳ Progress: {percent:.1f}% | Processed: {processed_count}/{total_groups} | ETA: {format_time(eta_seconds)}")

        if not DRY_RUN:
            time.sleep(0.2) # Fast mode

    total_time = time.time() - start_time
    log(f"🎉 CYCLE DONE! Processed {processed_count} groups in {format_time(total_time)}.")

def background_worker():
    while True:
        try:
            run_merge_process()
            log("Sleeping for 60 minutes...")
            time.sleep(3600) 
        except Exception as e:
            log(f"CRITICAL CRASH: {e}")
            time.sleep(60)

# Start Background Thread
threading.Thread(target=background_worker, daemon=True).start()

# Flask Server
@app.route('/')
def home():
    # Read the last few lines of the log file to show on the web page
    log_content = "No logs yet."
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f:
            # Get last 20 lines
            lines = f.readlines()[-20:]
            log_content = "<br>".join(lines)
            
    return f"<h1>Merge Script Running</h1><pre>{log_content}</pre>", 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
