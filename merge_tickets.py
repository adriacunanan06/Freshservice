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

# 🚨 LIVE MODE 🚨
DRY_RUN = False  

CHECKPOINT_FILE = "merge_checkpoint.json"
LOG_FILE = "merge_tickets.log"
# =================================================

app = Flask(__name__)
BASE_URL = f"https://{DOMAIN}/api/v2"
AUTH = (API_KEY, "X")
HEADERS = {"Content-Type": "application/json"}

# Logging Setup
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
        except: return set()
    return set()

def save_checkpoint(processed_set):
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump({"processed_requesters": list(processed_set)}, f)
    except: pass

def check_rate_limit(response):
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        log(f"⚠️ Rate limit hit. Sleeping for {retry_after}s...")
        time.sleep(retry_after + 5)
        return True
    return False

def get_all_tickets():
    all_tickets = []
    page = 1
    per_page = 100
    log("📥 Fetching tickets...")
    
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
            if page % 5 == 0: log(f"   Fetched {len(all_tickets)} tickets...")
            page += 1
        except:
            time.sleep(5)
            
    return all_tickets

def filter_valid_tickets(ticket_ids):
    """Checks a list of IDs and returns only the ones that exist."""
    valid_ids = []
    for tid in ticket_ids:
        try:
            # lightweight check
            res = requests.get(f"{BASE_URL}/tickets/{tid}", auth=AUTH)
            if res.status_code == 200:
                valid_ids.append(tid)
            elif res.status_code == 429:
                check_rate_limit(res)
                # Retry this specific ID check
                res = requests.get(f"{BASE_URL}/tickets/{tid}", auth=AUTH)
                if res.status_code == 200: valid_ids.append(tid)
        except:
            pass
    return valid_ids

def merge_tickets(primary_id, secondary_ids):
    url = f"{BASE_URL}/tickets/{primary_id}/merge"
    payload = { "secondary_ticket_ids": secondary_ids }
    
    if DRY_RUN: return True
            
    try:
        response = requests.put(url, auth=AUTH, headers=HEADERS, data=json.dumps(payload))
        
        if check_rate_limit(response): return merge_tickets(primary_id, secondary_ids)
            
        if response.status_code in [200, 204]:
            log(f"✅ Merged {len(secondary_ids)} into #{primary_id}")
            return True
            
        elif response.status_code == 404:
            # SMART RETRY LOGIC
            log(f"⚠️ Merge failed (404). Checking for deleted tickets in group...")
            
            # 1. Verify Primary
            try:
                p_check = requests.get(f"{BASE_URL}/tickets/{primary_id}", auth=AUTH)
                if p_check.status_code == 404:
                    log(f"   ❌ Primary Ticket #{primary_id} is gone. Cannot merge group.")
                    return False
            except: return False

            # 2. Filter Secondaries
            valid_secondary_ids = filter_valid_tickets(secondary_ids)
            
            if len(valid_secondary_ids) == 0:
                log(f"   ❌ All secondary tickets are gone. Nothing to merge.")
                return False
            
            if len(valid_secondary_ids) < len(secondary_ids):
                log(f"   🔄 Found {len(valid_secondary_ids)} valid tickets (removed {len(secondary_ids) - len(valid_secondary_ids)} bad ones). Retrying...")
                # RECURSIVE CALL with clean list
                return merge_tickets(primary_id, valid_secondary_ids)
            else:
                # If we are here, it means all tickets exist but 404 persists (very rare API glitch)
                log(f"   ❌ Unknown 404 error. Skipping.")
                return False

        else:
            log(f"❌ FAILED merge #{primary_id} | Status: {response.status_code} | Reason: {response.text}")
            return False
            
    except Exception as e:
        log(f"❌ Error merging: {e}")
        return False

def run_merge_process():
    log("========================================")
    log("STARTING MERGE PROCESS (SMART RETRY)")
    log("========================================")

    processed_requesters = load_checkpoint()
    tickets = get_all_tickets()
    log(f"📦 Total tickets: {len(tickets)}")
    
    log("🔍 Grouping...")
    tickets_by_requester = defaultdict(list)
    for t in tickets:
        tickets_by_requester[t['requester_id']].append(t)
    
    work_list = []
    for requester_id, user_tickets in tickets_by_requester.items():
        if str(requester_id) in processed_requesters or requester_id in processed_requesters: continue
        if len(user_tickets) < 2:
            processed_requesters.add(requester_id)
            continue
        work_list.append((requester_id, user_tickets))

    log(f"🚀 Found {len(work_list)} groups.")
    
    start_time = time.time()
    processed_count = 0
    total_groups = len(work_list)
    
    for i, (requester_id, user_tickets) in enumerate(work_list):
        user_tickets.sort(key=lambda x: x['created_at'])
        
        primary = user_tickets[-1] 
        secondary_tickets = user_tickets[:-1]
        secondary_ids = [t['id'] for t in secondary_tickets]
        
        success = merge_tickets(primary['id'], secondary_ids)
        
        if success:
            processed_requesters.add(requester_id)
            save_checkpoint(processed_requesters)
        
        processed_count += 1
        
        if processed_count % 5 == 0: 
            elapsed = time.time() - start_time
            if processed_count > 0:
                avg = elapsed / processed_count
                rem = total_groups - processed_count
                eta = rem * avg
                pct = (processed_count / total_groups) * 100
                log(f"⏳ Progress: {pct:.1f}% | Processed: {processed_count}/{total_groups} | ETA: {format_time(eta)}")

        if not DRY_RUN: time.sleep(1.0) 

    log(f"🎉 DONE! Processed {processed_count} groups.")

def background_worker():
    while True:
        try:
            run_merge_process()
            log("Sleeping 60 mins...")
            time.sleep(3600) 
        except: time.sleep(60)

threading.Thread(target=background_worker, daemon=True).start()

@app.route('/')
def home():
    content = "No logs."
    if os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'r') as f: content = "<br>".join(f.readlines()[-20:])
    return f"<h1>Merge Script (Smart Retry)</h1><pre>{content}</pre>", 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
