import threading
import time
import json
import logging
import os
import re
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
    except Exception as e: log(f"❌ Contact Error: {e}")
    return None

def fix_requester_if_needed(ticket_id, current_requester_id):
    if current_requester_id == SHOPIFY_SENDER_ID:
        log(f"   🔎 Ticket #{ticket_id} is from Shopify. Scanning body...")
        try:
            res = requests.get(f"{BASE_URL}/tickets/{ticket_id}?include=description", auth=AUTH)
            if res.status_code == 200:
                body = res.json().get('description_text', '')
                real_email = find_best_email(body)
                if real_email:
                    new_cid = get_or_create_contact(real_email)
                    if new_cid and not DRY_RUN:
                        requests.put(f"{BASE_URL}/tickets/{ticket_id}", auth=AUTH, headers=HEADERS, json={"requester_id": new_cid})
                        log(f"   ✅ Fixed #{ticket_id}: Requester -> {real_email}")
                        return new_cid
                else:
                    log(f"   ⚠️ No valid customer email found in body.")
        except Exception as e: log(f"❌ Fix Error: {e}")
    return current_requester_id

def merge_tickets(primary_id, secondary_ids):
    if DRY_RUN or not secondary_ids: return False
    # Correct Payload Structure
    url = f"{BASE_URL}/tickets/merge"
    payload = { "primary_id": primary_id, "ticket_ids": secondary_ids }
    try:
        res = requests.put(url, auth=AUTH, headers=HEADERS, data=json.dumps(payload))
        if check_rate_limit(res): return merge_tickets(primary_id, secondary_ids)
        if res.status_code in [200, 204]:
            log(f"   ⚡ Instant Merge: {secondary_ids} into #{primary_id}")
            return True
        else:
            log(f"   ❌ Merge Failed: {res.status_code} - {res.text}")
    except Exception as e: log(f"Merge Exception: {e}")
    return False

def get_available_agents():
    agents = []
    try:
        # Check Group 
        res = requests.get(f"{BASE_URL}/groups/{TARGET_GROUP_ID}/agents", auth=AUTH)
        if res.status_code == 200:
            for a in res.json():
                # Log who we found
                # log(f"   Found Agent: {a.get('id')} (Available: {a.get('available')})")
                if a.get("available", False): agents.append(a['id'])
        else:
            log(f"❌ Failed to fetch agents: {res.status_code} - {res.text}")
    except Exception as e: log(f"Agent Fetch Error: {e}")
    
    if not agents:
        log("   ⚠️ NO AGENTS ARE ONLINE/AVAILABLE RIGHT NOW.")
    else:
        log(f"   ✅ Found {len(agents)} online agents.")
        
    return agents

def process_single_ticket(ticket_id, requester_id):
    log(f"⚡ Processing Incoming Ticket #{ticket_id}...")
    
    # 1. FIX REQUESTER
    real_requester_id = fix_requester_if_needed(ticket_id, requester_id)
    
    # 2. MERGE CHECK
    query = f"requester_id:{real_requester_id} AND (status:2 OR status:3)"
    try:
        res = requests.get(f"{BASE_URL}/search/tickets?query=\"{query}\"", auth=AUTH)
        if res.status_code == 200:
            user_tickets = res.json().get('results', [])
            
            # Ensure current ticket is in list
            ids = [t['id'] for t in user_tickets]
            if ticket_id not in ids: 
                # Manually fetch current ticket to add to list
                curr = requests.get(f"{BASE_URL}/tickets/{ticket_id}", auth=AUTH).json()
                user_tickets.append(curr)

            if len(user_tickets) > 1:
                user_tickets.sort(key=lambda x: x['created_at'])
                primary = user_tickets[-1] 
                secondary = [t['id'] for t in user_tickets if t['id'] != primary['id']]
                
                log(f"   Found {len(user_tickets)} tickets for user. Merging...")
                if merge_tickets(primary['id'], secondary):
                    if ticket_id in secondary:
                        log(f"   🛑 Ticket #{ticket_id} was merged and deleted. Stop.")
                        return 
                    else:
                        ticket_id = primary['id']
    except Exception as e: log(f"Search/Merge Error: {e}")

    # 3. ASSIGN
    active_agents = get_available_agents()
    if active_agents:
        import random
        target_agent = random.choice(active_agents)
        if not DRY_RUN:
            res = requests.put(f"{BASE_URL}/tickets/{ticket_id}", auth=AUTH, headers=HEADERS, json={"responder_id": target_agent})
            if res.status_code == 200:
                log(f"   👮 Instant Assign #{ticket_id} -> Agent {target_agent}")
            else:
                log(f"   ❌ Assignment Failed: {res.status_code} - {res.text}")
    else:
        log(f"   ⚠️ Skipping assignment: No agents available.")

# --- WEBHOOK ---
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    t_id = data.get('ticket_id')
    r_id = data.get('requester_id')
    if t_id:
        threading.Thread(target=process_single_ticket, args=(t_id, r_id)).start()
        return "Processing", 200
    return "No ID", 400

# --- BACKGROUND WORKER (DISABLED FOR NOW TO FOCUS ON WEBHOOKS) ---
def background_worker():
    while True:
        log("💤 Service Ready (Waiting for Webhooks)...")
        time.sleep(3600) 

threading.Thread(target=background_worker, daemon=True).start()

@app.route('/')
def home():
    return "Auto-Dispatcher (DEBUG MODE) Running", 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
