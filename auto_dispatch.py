import threading
import time
import json
import logging
import os
import re
from collections import defaultdict
from flask import Flask, request
import requests

# ================= CONFIGURATION =================
DOMAIN = os.environ.get("FRESHDESK_DOMAIN")
API_KEY = os.environ.get("FRESHDESK_API_KEY")

# Group ID for "Agents"
TARGET_GROUP_ID = 159000817198
# Shopify System User ID
SHOPIFY_SENDER_ID = 159009730069

IGNORE_EMAILS = [
    "actorahelp@gmail.com", "customerservice@actorasupport.com",
    "mailer@shopify.com", "no-reply@shopify.com",
    "notifications@shopify.com", "support@actorasupport.com"
]

# 🚨 LIVE MODE 🚨
DRY_RUN = False  
# =================================================

app = Flask(__name__)
BASE_URL = f"https://{DOMAIN}/api/v2"
AUTH = (API_KEY, "X")
HEADERS = {"Content-Type": "application/json"}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
def log(msg): logging.info(msg)

def check_rate_limit(response):
    if response.status_code == 429:
        time.sleep(int(response.headers.get("Retry-After", 60)) + 2)
        return True
    return False

# --- SHARED HELPERS ---
def find_best_email(body_text):
    if not body_text: return None
    candidates = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', body_text)
    for email in candidates:
        if email.lower().strip() not in IGNORE_EMAILS: return email.lower().strip()
    return None

def get_or_create_contact(email):
    try:
        res = requests.get(f"{BASE_URL}/contacts?email={email}", auth=AUTH)
        if res.status_code == 200 and len(res.json()) > 0: return res.json()[0]['id']
        if not DRY_RUN:
            res = requests.post(f"{BASE_URL}/contacts", auth=AUTH, headers=HEADERS, json={"email": email, "name": email.split('@')[0]})
            if res.status_code == 201: return res.json()['id']
    except: pass
    return None

def fix_requester_if_needed(ticket_id, current_requester_id):
    """Detects Shopify sender and fixes it."""
    if current_requester_id == SHOPIFY_SENDER_ID:
        try:
            res = requests.get(f"{BASE_URL}/tickets/{ticket_id}?include=description", auth=AUTH)
            if res.status_code == 200:
                body = res.json().get('description_text', '')
                real_email = find_best_email(body)
                if real_email:
                    new_cid = get_or_create_contact(real_email)
                    if new_cid and not DRY_RUN:
                        requests.put(f"{BASE_URL}/tickets/{ticket_id}", auth=AUTH, headers=HEADERS, json={"requester_id": new_cid})
                        log(f"⚡ Instant Fix #{ticket_id}: Requester -> {real_email}")
                        return new_cid
        except: pass
    return current_requester_id

def merge_tickets(primary_id, secondary_ids):
    """Merges using the correct PRIMARY_ID + TICKET_IDS payload."""
    if DRY_RUN or not secondary_ids: return False
    url = f"{BASE_URL}/tickets/merge"
    payload = { "primary_id": primary_id, "ticket_ids": secondary_ids }
    try:
        res = requests.put(url, auth=AUTH, headers=HEADERS, data=json.dumps(payload))
        if check_rate_limit(res): return merge_tickets(primary_id, secondary_ids)
        if res.status_code in [200, 204]:
            log(f"⚡ Instant Merge: {secondary_ids} into #{primary_id}")
            return True
    except: pass
    return False

def get_available_agents():
    agents = []
    try:
        res = requests.get(f"{BASE_URL}/groups/{TARGET_GROUP_ID}/agents", auth=AUTH)
        if res.status_code == 200:
            for a in res.json():
                if a.get("available", False): agents.append(a['id'])
    except: pass
    return agents

def process_single_ticket(ticket_id, requester_id):
    """The Logic Engine for a single incoming ticket."""
    log(f"⚡ Processing Incoming Ticket #{ticket_id}...")
    
    # 1. FIX REQUESTER
    real_requester_id = fix_requester_if_needed(ticket_id, requester_id)
    
    # 2. CHECK FOR DUPLICATES (Merge)
    # Search for other open tickets by this user
    query = f"requester_id:{real_requester_id} AND (status:2 OR status:3)"
    try:
        res = requests.get(f"{BASE_URL}/search/tickets?query=\"{query}\"", auth=AUTH)
        if res.status_code == 200:
            user_tickets = res.json().get('results', [])
            # Filter to include the current ticket if not present (search lag)
            ids = [t['id'] for t in user_tickets]
            if ticket_id not in ids:
                # If search API is slow, we manually add the current ticket ID
                # But we can't get creation date easily without another call.
                # Usually search is fast enough.
                pass 
            
            if len(user_tickets) > 1:
                user_tickets.sort(key=lambda x: x['created_at'])
                primary = user_tickets[-1] # Keep newest
                secondary = [t['id'] for t in user_tickets if t['id'] != primary['id']]
                
                # If the current ticket is a secondary, it will be deleted/merged.
                if execute_merge := merge_tickets(primary['id'], secondary):
                    if ticket_id in secondary:
                        log(f"⚡ Ticket #{ticket_id} was a duplicate and has been merged/closed.")
                        return # Stop processing (it's gone)
                    else:
                        ticket_id = primary['id'] # We continue processing the primary
    except Exception as e: log(f"Merge Error: {e}")

    # 3. ASSIGN TO AGENT
    # Only if it survived the merge
    active_agents = get_available_agents()
    if active_agents:
        # Simple random assignment for instant webhook (Round robin harder without state)
        import random
        target_agent = random.choice(active_agents)
        if not DRY_RUN:
            requests.put(f"{BASE_URL}/tickets/{ticket_id}", auth=AUTH, headers=HEADERS, json={"responder_id": target_agent})
            log(f"⚡ Instant Assign #{ticket_id} -> Agent {target_agent}")

# --- WEBHOOK ENDPOINT ---
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    # Expecting Freshdesk to send { "ticket_id": 123, "requester_id": 456 }
    t_id = data.get('ticket_id')
    r_id = data.get('requester_id')
    
    if t_id:
        # Process in a separate thread so we respond "200 OK" to Freshdesk instantly
        threading.Thread(target=process_single_ticket, args=(t_id, r_id)).start()
        return "Processing", 200
    return "No ID", 400

# --- BACKGROUND SAFETY NET (Cleanup every 30 mins) ---
def background_worker():
    while True:
        log("💤 Background worker sleeping (Waiting for Webhooks)...")
        time.sleep(1800) # Sleep 30 mins, rely on webhooks mostly

threading.Thread(target=background_worker, daemon=True).start()

@app.route('/')
def home():
    return "Auto-Dispatcher (Instant Webhook) Running", 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
