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
        retry = int(response.headers.get("Retry-After", 60))
        log(f"⚠️ Rate limit hit. Sleeping {retry}s...")
        time.sleep(retry + 5)
        return True
    return False

# --- HELPER FUNCTIONS ---
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

def fix_requester_if_needed(ticket):
    tid = ticket['id']
    req_id = ticket['requester_id']
    if req_id == SHOPIFY_SENDER_ID:
        try:
            res = requests.get(f"{BASE_URL}/tickets/{tid}?include=description", auth=AUTH)
            if res.status_code == 200:
                body = res.json().get('description_text', '')
                real_email = find_best_email(body)
                if real_email:
                    new_cid = get_or_create_contact(real_email)
                    if new_cid and not DRY_RUN:
                        requests.put(f"{BASE_URL}/tickets/{tid}", auth=AUTH, headers=HEADERS, json={"requester_id": new_cid})
                        log(f"   🔧 Fixed #{tid}: Requester -> {real_email}")
                        return new_cid
        except: pass
    return req_id

def merge_tickets(primary_id, secondary_ids):
    if DRY_RUN or not secondary_ids: return False
    url = f"{BASE_URL}/tickets/merge"
    # CORRECT PAYLOAD (Confirmed by Support)
    payload = { "primary_id": primary_id, "ticket_ids": secondary_ids }
    try:
        res = requests.put(url, auth=AUTH, headers=HEADERS, data=json.dumps(payload))
        if check_rate_limit(res): return merge_tickets(primary_id, secondary_ids)
        if res.status_code in [200, 204]:
            log(f"   ⚡ Instant Merge: {secondary_ids} into #{primary_id}")
            return True
    except: pass
    return False

def get_available_agents():
    """
    Fetches agents. If none are explicitly 'Available' (toggle missing),
    it falls back to 'All Agents in Group'.
    """
    all_agents = []
    available_agents = []
    
    try:
        res = requests.get(f"{BASE_URL}/groups/{TARGET_GROUP_ID}/agents", auth=AUTH)
        if res.status_code == 200:
            for a in res.json():
                all_agents.append(a['id'])
                if a.get("available", False): 
                    available_agents.append(a['id'])
    except: pass

    if available_agents:
        return available_agents
    else:
        # FALLBACK: If API says nobody is online (because toggle is missing),
        # assume EVERYONE is working and return the full list.
        if all_agents:
            # log(f"   ⚠️ No 'Available' status found. Using FORCE MODE: Distributing to all {len(all_agents)} agents.")
            return all_agents
        return []

# --- CORE LOGIC ---
def process_single_ticket(ticket_object, active_agents=None):
    t_id = ticket_object['id']
    real_req_id = fix_requester_if_needed(ticket_object)
    
    # MERGE CHECK
    query = f"requester_id:{real_req_id} AND (status:2 OR status:3)"
    try:
        res = requests.get(f"{BASE_URL}/search/tickets?query=\"{query}\"", auth=AUTH)
        if res.status_code == 200:
            user_tickets = res.json().get('results', [])
            ids = [t['id'] for t in user_tickets]
            if t_id not in ids: user_tickets.append(ticket_object)

            if len(user_tickets) > 1:
                user_tickets.sort(key=lambda x: x['created_at'])
                primary = user_tickets[-1] 
                secondary = [t['id'] for t in user_tickets if t['id'] != primary['id']]
                
                if merge_tickets(primary['id'], secondary):
                    if t_id in secondary:
                        log(f"   🛑 Ticket #{t_id} merged/deleted. Done.")
                        return 
                    else:
                        t_id = primary['id']
    except Exception as e: log(f"Merge Error: {e}")

    # ASSIGNMENT
    if not active_agents: active_agents = get_available_agents()
    
    current_responder = ticket_object.get('responder_id')
    should_assign = False
    
    if current_responder is None: should_assign = True
    elif current_responder not in active_agents: should_assign = True
    
    if should_assign and active_agents:
        import random
        target = random.choice(active_agents)
        if not DRY_RUN:
            requests.put(f"{BASE_URL}/tickets/{t_id}", auth=AUTH, headers=HEADERS, json={"responder_id": target})
            log(f"   👮 Assigned #{t_id} -> Agent {target}")

# --- WEBHOOK ---
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    t_id = data.get('ticket_id')
    if t_id:
        try:
            res = requests.get(f"{BASE_URL}/tickets/{t_id}", auth=AUTH)
            if res.status_code == 200:
                ticket_obj = res.json()
                threading.Thread(target=process_single_ticket, args=(ticket_obj,)).start()
        except: pass
    return "OK", 200

# --- SWEEPER ---
def run_backlog_sweep():
    log("🧹 STARTING BACKLOG SWEEP...")
    active_agents = get_available_agents()
    if not active_agents: 
        log("⚠️ CRITICAL: No agents found in group at all!")
        return

    page = 1
    query = f"group_id:{TARGET_GROUP_ID} AND (status:2 OR status:3)"
    while True:
        try:
            res = requests.get(f"{BASE_URL}/search/tickets?query=\"{query}\"&page={page}", auth=AUTH)
            if check_rate_limit(res): continue
            if res.status_code != 200: break
            tickets = res.json().get('results', [])
            if not tickets: break
            log(f"   🔎 Sweeping Batch {page}: {len(tickets)} tickets...")
            for ticket in tickets:
                process_single_ticket(ticket, active_agents)
                time.sleep(0.2)
            if len(tickets) < 30: break 
            page += 1
        except: break
    log("✅ Sweep Complete.")

def background_worker():
    time.sleep(5) 
    run_backlog_sweep()
    while True:
        log("💤 Sweeper sleeping 10 mins...")
        time.sleep(600)
        run_backlog_sweep()

threading.Thread(target=background_worker, daemon=True).start()

@app.route('/')
def home():
    return "Auto-Dispatcher (FORCE MODE) Running", 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
