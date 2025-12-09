import threading
import time
import json
import logging
import os
import re
from flask import Flask, request
import requests

# ================= CONFIGURATION =================
# Freshdesk
FD_DOMAIN = os.environ.get("FRESHDESK_DOMAIN")
FD_API_KEY = os.environ.get("FRESHDESK_API_KEY")

# Clockify (Hardcoded Correct Key)
CLOCK_API_KEY = "d200b163-0e11-49be-a6dd-fa0e01682d08"

# Agents (Jean, Lance, Vanesa)
AGENT_IDS = [159009628844, 159009628874, 159009628889]
TARGET_GROUP_NAME = "Agents" 
SHOPIFY_SENDER_ID = 159009730069

IGNORE_EMAILS = [
    "actorahelp@gmail.com", "customerservice@actorasupport.com", 
    "mailer@shopify.com", "no-reply@shopify.com", 
    "notifications@shopify.com", "support@actorasupport.com"
]

DRY_RUN = False  
# =================================================

app = Flask(__name__)
FD_BASE_URL = f"https://{FD_DOMAIN}/api/v2"
FD_AUTH = (FD_API_KEY, "X")
FD_HEADERS = {"Content-Type": "application/json"}
CLOCK_HEADERS = {"X-Api-Key": CLOCK_API_KEY}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
def log(msg): logging.info(msg)

CACHED_GROUP_ID = None
CACHED_CLOCKIFY_MAP = {} 

def check_rate_limit(response):
    if response.status_code == 429:
        retry = int(response.headers.get("Retry-After", 60))
        log(f"⚠️ Rate limit hit. Sleeping {retry}s...")
        time.sleep(retry + 5)
        return True
    return False

# --- CLOCKIFY LOGIC ---
def build_clockify_cache():
    global CACHED_CLOCKIFY_MAP
    CACHED_CLOCKIFY_MAP = {}
    
    log("🏗️ Building Clockify User Cache...")
    try:
        res = requests.get("https://api.clockify.me/api/v1/workspaces", headers=CLOCK_HEADERS)
        if res.status_code != 200:
            log(f"❌ Clockify Error: {res.status_code} - {res.text}")
            return

        workspaces = res.json()
        for ws in workspaces:
            ws_id = ws['id']
            # Fetch users
            res_u = requests.get(f"https://api.clockify.me/api/v1/workspaces/{ws_id}/users", headers=CLOCK_HEADERS)
            if res_u.status_code == 200:
                for u in res_u.json():
                    CACHED_CLOCKIFY_MAP[u['email'].lower()] = (ws_id, u['id'])
        
        log(f"✅ Clockify Cache Built ({len(CACHED_CLOCKIFY_MAP)} users).")
    except Exception as e: log(f"Clockify Init Error: {e}")

def is_user_clocked_in(email):
    email = email.lower()
    if not CACHED_CLOCKIFY_MAP: build_clockify_cache()
    
    if email not in CACHED_CLOCKIFY_MAP: return False
    
    ws_id, user_id = CACHED_CLOCKIFY_MAP[email]
    try:
        url = f"https://api.clockify.me/api/v1/workspaces/{ws_id}/user/{user_id}/time-entries?in-progress=true"
        res = requests.get(url, headers=CLOCK_HEADERS)
        if res.status_code == 200:
            if len(res.json()) > 0:
                log(f"      🟢 {email} is CLOCKED IN.")
                return True
    except: pass
    return False

def get_active_agents_via_clockify():
    active_list = []
    # Force cache rebuild if empty to be safe
    if not CACHED_CLOCKIFY_MAP: build_clockify_cache()

    for agent_id in AGENT_IDS:
        try:
            res = requests.get(f"{FD_BASE_URL}/agents/{agent_id}", auth=FD_AUTH)
            if res.status_code == 200:
                email = res.json()['contact']['email']
                if is_user_clocked_in(email): active_list.append(agent_id)
        except: pass
    
    return active_list

# --- FRESHDESK HELPERS ---
def get_group_id():
    global CACHED_GROUP_ID
    if CACHED_GROUP_ID: return CACHED_GROUP_ID
    try:
        res = requests.get(f"{FD_BASE_URL}/groups", auth=FD_AUTH)
        if res.status_code == 200:
            for g in res.json():
                if g['name'].lower() == TARGET_GROUP_NAME.lower():
                    CACHED_GROUP_ID = g['id']
                    return CACHED_GROUP_ID
    except: pass
    return None

def find_best_email(body_text):
    if not body_text: return None
    candidates = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', body_text)
    for email in candidates:
        if email.lower().strip() not in IGNORE_EMAILS: return email.lower().strip()
    return None

def get_or_create_contact(email):
    try:
        res = requests.get(f"{FD_BASE_URL}/contacts?email={email}", auth=FD_AUTH)
        if res.status_code == 200 and len(res.json()) > 0: return res.json()[0]['id']
        if not DRY_RUN:
            res = requests.post(f"{FD_BASE_URL}/contacts", auth=FD_AUTH, headers=FD_HEADERS, json={"email": email, "name": email.split('@')[0]})
            if res.status_code == 201: return res.json()['id']
    except: pass
    return None

def fix_requester_if_needed(ticket):
    tid = ticket['id']
    req_id = ticket['requester_id']
    if req_id == SHOPIFY_SENDER_ID:
        try:
            res = requests.get(f"{FD_BASE_URL}/tickets/{tid}?include=description", auth=FD_AUTH)
            if res.status_code == 200:
                body = res.json().get('description_text', '')
                real_email = find_best_email(body)
                if real_email:
                    new_cid = get_or_create_contact(real_email)
                    if new_cid and not DRY_RUN:
                        requests.put(f"{FD_BASE_URL}/tickets/{tid}", auth=FD_AUTH, headers=FD_HEADERS, json={"requester_id": new_cid})
                        log(f"   🔧 Fixed #{tid}: Requester -> {real_email}")
                        return new_cid
        except: pass
    return req_id

def merge_tickets(primary_id, secondary_ids):
    if DRY_RUN or not secondary_ids: return False
    url = f"{FD_BASE_URL}/tickets/merge"
    payload = { "primary_id": primary_id, "ticket_ids": secondary_ids }
    try:
        res = requests.put(url, auth=FD_AUTH, headers=FD_HEADERS, data=json.dumps(payload))
        if check_rate_limit(res): return merge_tickets(primary_id, secondary_ids)
        if res.status_code in [200, 204]:
            log(f"   ⚡ Instant Merge: {secondary_ids} into #{primary_id}")
            return True
        else:
            log(f"   ❌ Merge Failed: {res.text}")
    except Exception as e: log(f"Merge Err: {e}")
    return False

# --- CORE LOGIC ---
def process_single_ticket(ticket_object):
    t_id = ticket_object['id']
    
    # 1. FIX REQUESTER
    real_req_id = fix_requester_if_needed(ticket_object)
    
    # 2. MERGE CHECK
    query = f"requester_id:{real_req_id} AND (status:2 OR status:3)"
    try:
        res = requests.get(f"{FD_BASE_URL}/search/tickets?query=\"{query}\"", auth=FD_AUTH)
        if res.status_code == 200:
            user_tickets = res.json().get('results', [])
            ids = [t['id'] for t in user_tickets]
            if t_id not in ids: user_tickets.append(ticket_object)

            if len(user_tickets) > 1:
                user_tickets.sort(key=lambda x: x['created_at'])
                primary = user_tickets[-1] 
                secondary = [t['id'] for t in user_tickets if t['id'] != primary['id']]
                
                log(f"   🔄 Merging {len(secondary)} duplicates into #{primary['id']}...")
                if merge_tickets(primary['id'], secondary):
                    if t_id in secondary: return 
                    else: t_id = primary['id']
    except Exception as e: log(f"Merge Error: {e}")

    # 3. ASSIGN (Via Clockify)
    active_agents = get_active_agents_via_clockify()
    group_id = get_group_id()
    
    current_responder = ticket_object.get('responder_id')
    should_assign = False
    
    if current_responder is None: 
        should_assign = True
    elif current_responder in AGENT_IDS and current_responder not in active_agents:
        should_assign = True
    
    if should_assign:
        if active_agents:
            import random
            target = random.choice(active_agents)
            if not DRY_RUN:
                payload = { "responder_id": target }
                if group_id: payload["group_id"] = int(group_id)
                requests.put(f"{FD_BASE_URL}/tickets/{t_id}", auth=FD_AUTH, headers=FD_HEADERS, json=payload)
                log(f"   👮 Assigned #{t_id} -> Agent {target} (Clocked In)")

# --- WEBHOOK ---
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    t_id = data.get('ticket_id')
    if t_id:
        try:
            res = requests.get(f"{FD_BASE_URL}/tickets/{t_id}", auth=FD_AUTH)
            if res.status_code == 200:
                ticket_obj = res.json()
                threading.Thread(target=process_single_ticket, args=(ticket_obj,)).start()
        except: pass
    return "OK", 200

# --- SWEEPER ---
def run_backlog_sweep():
    log("🧹 STARTING BACKLOG SWEEP...")
    active = get_active_agents_via_clockify()
    if not active: log("   ⚠️ No agents clocked in. Merging only.")
    else: log(f"   ✅ Agents Clocked In: {len(active)}")

    group_id = get_group_id()
    if not group_id: return

    page = 1
    query = f"group_id:{group_id} AND (status:2 OR status:3)"
    while True:
        try:
            res = requests.get(f"{FD_BASE_URL}/search/tickets?query=\"{query}\"&page={page}", auth=FD_AUTH)
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
    return "Auto-Dispatcher (HARDCODED KEY) Running", 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
