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

# The 3 Agents we trust (Jean, Lance, Vanesa)
AGENT_IDS = [
    159009628844, 
    159009628874, 
    159009628889
]

TARGET_GROUP_NAME = "Agents" 
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

# Global Cache
CACHED_GROUP_ID = None

def check_rate_limit(response):
    if response.status_code == 429:
        retry = int(response.headers.get("Retry-After", 60))
        log(f"⚠️ Rate limit hit. Sleeping {retry}s...")
        time.sleep(retry + 5)
        return True
    return False

# --- DYNAMIC GROUP LOOKUP ---
def get_group_id():
    global CACHED_GROUP_ID
    if CACHED_GROUP_ID: return CACHED_GROUP_ID
    
    # log(f"🔎 Looking for Group ID for '{TARGET_GROUP_NAME}'...")
    try:
        res = requests.get(f"{BASE_URL}/groups", auth=AUTH)
        if res.status_code == 200:
            groups = res.json()
            for g in groups:
                if g['name'].lower() == TARGET_GROUP_NAME.lower():
                    CACHED_GROUP_ID = g['id']
                    # log(f"   ✅ Found Group '{g['name']}' -> ID: {CACHED_GROUP_ID}")
                    return CACHED_GROUP_ID
    except: pass
    return None

# --- AGENT AVAILABILITY CHECKER ---
def get_active_agents():
    """
    Checks the specific 3 agents to see who is actually 'Available'.
    """
    active_list = []
    # log("🔎 Checking Agent Availability...")
    
    for agent_id in AGENT_IDS:
        try:
            res = requests.get(f"{BASE_URL}/agents/{agent_id}", auth=AUTH)
            if res.status_code == 200:
                data = res.json()
                name = data['contact']['name']
                is_available = data.get('available', False)
                
                if is_available:
                    # log(f"   🟢 {name} is ONLINE.")
                    active_list.append(agent_id)
                else:
                    # log(f"   🔴 {name} is AWAY/OFFLINE.")
                    pass
        except: pass
        
    return active_list

# --- HELPERS ---
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
    payload = { "primary_id": primary_id, "ticket_ids": secondary_ids }
    try:
        res = requests.put(url, auth=AUTH, headers=HEADERS, data=json.dumps(payload))
        if check_rate_limit(res): return merge_tickets(primary_id, secondary_ids)
        if res.status_code in [200, 204]:
            log(f"   ⚡ Instant Merge: {secondary_ids} into #{primary_id}")
            return True
    except: pass
    return False

# --- CORE LOGIC ---
def process_single_ticket(ticket_object):
    t_id = ticket_object['id']
    
    # 1. FIX REQUESTER
    real_req_id = fix_requester_if_needed(ticket_object)
    
    # 2. MERGE CHECK
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
                        log(f"   🛑 Ticket #{t_id} merged. Done.")
                        return 
                    else:
                        t_id = primary['id']
    except Exception as e: log(f"Merge Error: {e}")

    # 3. ASSIGN (Check Availability First)
    current_responder = ticket_object.get('responder_id')
    
    # Get the "Agents" Group ID to enforce grouping
    group_id = get_group_id()
    
    # Who is online RIGHT NOW?
    active_agents = get_active_agents()
    
    should_assign = False
    
    # If unassigned OR assigned to someone who is currently OFFLINE
    if current_responder is None: 
        should_assign = True
    elif current_responder not in active_agents:
        # If the assigned person went offline, we reassign (optional, but requested "available only")
        # should_assign = True 
        # Actually, let's only reassign if they are NOT in the allowed list at all
        # Or if you want strict "Only Available" enforcement:
        if current_responder in AGENT_IDS and current_responder not in active_agents:
             # They are one of our 3, but they are offline.
             # Strict mode: Reassign to someone online.
             should_assign = True
    
    if should_assign:
        if active_agents:
            import random
            target = random.choice(active_agents)
            if not DRY_RUN:
                payload = { "responder_id": target }
                # Force the group ID if we know it
                if group_id: payload["group_id"] = int(group_id)
                
                requests.put(f"{BASE_URL}/tickets/{t_id}", auth=AUTH, headers=HEADERS, json=payload)
                log(f"   👮 Assigned #{t_id} -> Agent {target} (Group: Agents)")
        else:
             log(f"   ⚠️ Cannot assign #{t_id}: All 3 agents are currently OFFLINE.")

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
    log("🧹 STARTING BACKLOG SWEEP (Old & New Tickets)...")
    
    group_id = get_group_id()
    if not group_id: 
        log("❌ Critical: Cannot find group 'Agents'. Retrying later.")
        return

    page = 1
    # Fetch ALL Open/Pending tickets in the 'Agents' group
    query = f"group_id:{group_id} AND (status:2 OR status:3)"
    
    while True:
        try:
            res = requests.get(f"{BASE_URL}/search/tickets?query=\"{query}\"&page={page}", auth=AUTH)
            if check_rate_limit(res): continue
            if res.status_code != 200: break
            tickets = res.json().get('results', [])
            if not tickets: break
            log(f"   🔎 Sweeping Batch {page}: {len(tickets)} tickets...")
            for ticket in tickets:
                process_single_ticket(ticket)
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
    return "Auto-Dispatcher (AVAILABLE AGENTS ONLY) Running", 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
