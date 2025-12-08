import threading
import time
import json
import logging
import os
from collections import defaultdict
from flask import Flask
import requests

# ================= CONFIGURATION (SAFE MODE) =================
# We read these from the Cloud Server settings
DOMAIN = os.environ.get("FRESHDESK_DOMAIN") 
API_KEY = os.environ.get("FRESHDESK_API_KEY")

# Hardcoded logic is fine, but keys must be hidden
ACTORA_SENDER_ID = 159009728889 
SHOPIFY_SENDER_ID = 159009730069
IGNORE_EMAILS = [
    "actorahelp@gmail.com", "customerservice@actorasupport.com",
    "mailer@shopify.com", "no-reply@shopify.com",
    "notifications@shopify.com", "support@actorasupport.com"
]
# SAFETY SWITCH
DRY_RUN = True  
CHECKPOINT_FILE = "merge_checkpoint.json"
# =================================================

app = Flask(__name__)
BASE_URL = f"https://{DOMAIN}/api/v2"
AUTH = (API_KEY, "X")
HEADERS = {"Content-Type": "application/json"}

# Use standard print for logs on Render (it captures stdout)
def log(msg):
    print(f"[LOG] {msg}")

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
        time.sleep(retry_after + 2)
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
            if page % 5 == 0: # Log every 5 pages to reduce noise
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
        log(f"   [DRY RUN] Would merge {len(secondary_ids)} tickets ({secondary_ids}) INTO -> #{primary_id}")
        return True
            
    try:
        response = requests.put(url, auth=AUTH, headers=HEADERS, data=json.dumps(payload))
        if check_rate_limit(response): return merge_tickets(primary_id, secondary_ids) # Retry
            
        if response.status_code in [200, 204]:
            log(f"✅ SUCCESS: Merged {len(secondary_ids)} tickets INTO -> #{primary_id}")
            return True
        else:
            log(f"❌ FAILED to merge into #{primary_id}: {response.text}")
            return False
    except Exception as e:
        log(f"❌ Error during merge request: {e}")
        return False

def run_merge_process():
    """Main Logic Loop"""
    log("========================================")
    log("STARTING TICKET MERGE PROCESS")
    log(f"Mode: {'DRY RUN (Safe)' if DRY_RUN else 'LIVE (Destructive)'}")
    log("========================================")

    processed_requesters = load_checkpoint()
    if processed_requesters:
        log(f"🔄 Checkpoint found! Skipping {len(processed_requesters)} requesters.")

    tickets = get_all_tickets()
    log(f"📦 Total tickets fetched: {len(tickets)}")
    
    log("🔍 Grouping tickets by requester...")
    tickets_by_requester = defaultdict(list)
    
    for t in tickets:
        tickets_by_requester[t['requester_id']].append({
            'id': t['id'],
            'created_at': t['created_at']
        })
        
    log(f"   Found {len(tickets_by_requester)} unique requesters.")
    log("🚀 Starting Merge Process (Oldest -> Newest)...")
    
    merge_count = 0
    
    for requester_id, user_tickets in tickets_by_requester.items():
        if str(requester_id) in processed_requesters or requester_id in processed_requesters:
            continue

        if len(user_tickets) < 2:
            processed_requesters.add(requester_id)
            continue 
            
        user_tickets.sort(key=lambda x: x['created_at'])
        
        primary = user_tickets[-1] 
        secondary_tickets = user_tickets[:-1]
        secondary_ids = [t['id'] for t in secondary_tickets]
        
        log(f"➡️ Processing Requester {requester_id}: Found {len(user_tickets)} tickets.")
        
        success = merge_tickets(primary['id'], secondary_ids)
        
        if success:
            merge_count += 1
            processed_requesters.add(requester_id)
            save_checkpoint(processed_requesters)
        
        if not DRY_RUN:
            time.sleep(1)

    log(f"🎉 CYCLE DONE! Merged {merge_count} groups.")

def background_worker():
    """Runs the merge process every 60 minutes so it keeps checking forever."""
    while True:
        try:
            run_merge_process()
            log("Sleeping for 60 minutes before next check...")
            time.sleep(3600) 
        except Exception as e:
            log(f"CRITICAL WORKER CRASH: {e}")
            time.sleep(60)

# Start the background thread
threading.Thread(target=background_worker, daemon=True).start()

# Web Server Route (Required for Render)
@app.route('/')
def home():
    return "Merge Script is Running in Background!", 200

if __name__ == "__main__":
    # Render sets the PORT environment variable
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
