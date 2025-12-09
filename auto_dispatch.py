import threading
import time
import json
import logging
import os
import re
import queue
from datetime import datetime
from flask import Flask, request
import requests

# ================= CONFIGURATION =================
# 1. Try to get keys from Environment (Render)
FD_DOMAIN = os.environ.get("FRESHDESK_DOMAIN")
FD_API_KEY = os.environ.get("FRESHDESK_API_KEY")
CLOCK_API_KEY = os.environ.get("CLOCKIFY_API_KEY")

# 2. Fallback to Hardcoded Keys (Local Testing)
if not FD_DOMAIN: FD_DOMAIN = "actorasupport.freshdesk.com"
if not FD_API_KEY: FD_API_KEY = "nToPJRmvqzHHWJ6pib36"
if not CLOCK_API_KEY: CLOCK_API_KEY = "MWFlZWY4MDctYWE5Zi00NDFkLWE1ODItMzg5OGQ0MmYwY2Uy"

# Agents
AGENT_IDS = [
    159009628895, # Cyril Poche
    159009628844, # Jean Kreanne Dawatan
    159009628874, # Lance Anthony
    159009628889  # Vanesa Joy Roble
]

TARGET_GROUP_NAME = "Agents" 
SHOPIFY_SENDER_ID = 159009730069

IGNORE_EMAILS = [
    "actorahelp@gmail.com", "customerservice@actorasupport.com", 
    "mailer@shopify.com", "no-reply@shopify.com", 
    "notifications@shopify.com", "support@actorasupport.com"
]

DRY_RUN = False  
# =======================================================

app = Flask(__name__)
FD_BASE_URL = f"https://{FD_DOMAIN}/api/v2"
FD_AUTH = (FD_API_KEY, "X")
FD_HEADERS = {"Content-Type": "application/json"}
CLOCK_HEADERS = {"X-Api-Key": CLOCK_API_KEY}

# GLOBAL QUEUE (The Waiting Line)
TICKET_QUEUE = queue.Queue()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
def log(msg): logging.info(msg)

CACHED_GROUP_ID = None
CACHED_WORKSPACE_ID = None
CACHED_CLOCKIFY_MAP = {} 

def check_rate_limit(response):
    if response.status_code == 429:
        retry = int(response.headers.get("Retry-After", 60))
        log(f"⚠️ Rate limit hit. Pausing worker for {retry}s...")
        time.sleep(retry + 5)
        return True
    return False

# --- CLOCKIFY LOGIC ---
def init_clockify():
    global CACHED_WORKSPACE_ID
    # log("🔑 Authenticating with Clockify...")
    try:
        res = requests.get("https://api.clockify.me/api/v1/user", headers=CLOCK_HEADERS)
        if res.status_code == 200:
            user_data = res.json()
            default_ws = user_data.get('defaultWorkspace')
            active_ws = user_data.get('activeWorkspace')
            CACHED_WORKSPACE_ID = default_ws if default_ws else active_ws
            return True
    except: pass
    return False

def build_clockify_cache():
    global CACHED_CLOCKIFY_MAP
    CACHED_CLOCKIFY_MAP = {}
    if not CACHED_WORKSPACE_ID:
        if not init_clockify(): return

    try:
        res = requests.get(f"https://api.clockify.me/api/v1/workspaces/{CACHED_WORKSPACE_ID}/users", headers=CLOCK_HEADERS)
        if res.status_code == 200:
            for u in res.json():
                CACHED_CLOCKIFY_MAP[u['email'].lower()] = u['id']
            log(f"✅ Clockify Cache Updated ({len(CACHED_CLOCKIFY_MAP)} users).")
    except: pass

def is_user_clocked_in(email):
    email = email.lower()
    if not CACHED_CLOCKIFY_MAP: build_clockify_cache()
    if email not in CACHED_CLOCKIFY_MAP: return False
    
    ws_id = CACHED_WORKSPACE_ID
    user_id = CACHED_CLOCKIFY_MAP[email]
    try:
        url = f"https://api.clockify.me/api/v1/workspaces/{ws_id}/user/{user_id}/time-entries?in-progress=true"
        res = requests.get(url, headers=CLOCK_HEADERS)
        if res.status_code == 200:
            entries = res.json()
            if len(entries) > 0:
                start_time = entries[0].get('timeInterval', {}).get('start', 'Unknown')
                # log(f"      🟢 {email} is ONLINE (Started: {start_time})")
                return True
    except: pass
    return False

def get_active_agents_via_clockify():
    active_list = []
    # Refresh cache occasionally or if empty
    if not CACHED_CLOCKIFY_MAP: build_clockify_cache()
    
    for agent_id in AGENT_IDS:
        try:
            res = requests.get(f"{FD_BASE_URL}/agents/{agent_id}", auth=FD_AUTH)
            if res.status_code == 200:
                primary_email = res.json()['contact']['email']
                is_active = is_user_clocked_in(primary_email)
                
                # CYRIL DUAL-EMAIL CHECK
                if agent_id == 159009628895 and not is_active:
                    secondary = "angilynbueno.gbarealty@gmail.com"
                    if is_user_clocked_in(secondary): is_active = True
                
                if is_active: active_list.append(agent_id)
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
    except: pass
    return False

# --- LOGIC PIPELINE ---
def fix_requester_if_needed(ticket):
    tid = ticket['id']
    req_id = ticket['requester_id']
    
    if req_id == SHOPIFY_SENDER_ID:
        try:
            res = requests.get(f"{FD_BASE_URL}/tickets/{tid}?include=description", auth=FD_AUTH)
            if check_rate_limit(res): return fix_requester_if_needed(ticket)
            
            if res.status_code == 200:
                body = res.json().get('description_text', '')
                real_email = find_best_email(body)
                if real_email:
                    new_cid = get_or_create_contact(real_email)
                    if new_cid and not DRY_RUN:
                        requests.put(f"{FD_BASE_URL}/tickets/{tid}", auth=FD_AUTH, headers=FD_HEADERS, json={"requester_id": new_cid})
                        log(f"   ✅ Fixed Requester -> {real_email}")
                        return new_cid
        except: pass
    return req_id

def perform_merge_check(t_id, requester_id, ticket_object):
    query = f"requester_id:{requester_id} AND (status:2 OR status:3)"
    try:
        res = requests.get(f"{FD_BASE_URL}/search/tickets?query=\"{query}\"", auth=FD_AUTH)
        if check_rate_limit(res): return perform_merge_check(t_id, requester_id, ticket_object)
        
        if res.status_code == 200:
            user_tickets = res.json().get('results', [])
            ids = [t['id'] for t in user_tickets]
            if t_id not in ids: user_tickets.append(ticket_object)

            if len(user_tickets) > 1:
                user_tickets.sort(key=lambda x: x['created_at'])
                primary = user_tickets[-1] 
                secondary = [t['id'] for t in user_tickets if t['id'] != primary['id']]
                
                log(f"   🔄 Merging {len(secondary)} duplicates...")
                if merge_tickets(primary['id'], secondary):
                    if t_id in secondary: return None 
                    else: return primary['id'] 
    except: pass
    return t_id

def assign_group_and_agent(t_id, current_group_id, current_responder_id):
    target_group_id = get_group_id()
    if not target_group_id: return

    active_agents = get_active_agents_via_clockify()
    target_responder = None
    
    if current_responder_id in active_agents:
        target_responder = current_responder_id
    else:
        if active_agents:
            import random
            target_responder = random.choice(active_agents)
        else:
            target_responder = current_responder_id 

    payload = {}
    if current_group_id != target_group_id: payload['group_id'] = target_group_id
    if target_responder != current_responder_id:
        payload['responder_id'] = target_responder
        payload['group_id'] = target_group_id 

    if payload:
        if not DRY_RUN:
            res = requests.put(f"{FD_BASE_URL}/tickets/{t_id}", auth=FD_AUTH, headers=FD_HEADERS, json=payload)
            if check_rate_limit(res): return # Simple skip if limited on assign
            if res.status_code == 200:
                agent_msg = f" -> Agent {target_responder}" if target_responder else " (No Online Agents)"
                log(f"   ✅ Assigned #{t_id} -> Group 'Agents'{agent_msg}")

def process_single_ticket(ticket_object):
    t_id = ticket_object['id']
    log(f"⚡ Processing #{t_id}...")

    real_req_id = fix_requester_if_needed(ticket_object)
    surviving_id = perform_merge_check(t_id, real_req_id, ticket_object)
    
    if surviving_id:
        try:
            res = requests.get(f"{FD_BASE_URL}/tickets/{surviving_id}", auth=FD_AUTH)
            if check_rate_limit(res): return
            if res.status_code == 200:
                upd = res.json()
                assign_group_and_agent(surviving_id, upd.get('group_id'), upd.get('responder_id'))
        except: pass

# --- WORKER THREAD (THE QUEUE PROCESSOR) ---
def worker():
    while True:
        ticket_data = TICKET_QUEUE.get()
        try:
            process_single_ticket(ticket_data)
        except Exception as e:
            log(f"Worker Error: {e}")
        finally:
            TICKET_QUEUE.task_done()
            time.sleep(2) # Safe delay between tickets to avoid Rate Limits

# --- WEBHOOK ---
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    t_id = data.get('ticket_id')
    if t_id:
        # Instead of processing immediately, put it in the Waiting Line
        # We construct a minimal ticket object for the queue
        ticket_obj = {'id': t_id, 'requester_id': data.get('requester_id')}
        TICKET_QUEUE.put(ticket_obj)
        return "Queued", 200
    return "No ID", 400

# --- SWEEPER ---
def run_backlog_sweep():
    log("🧹 STARTING BACKLOG SWEEP...")
    group_id = get_group_id()
    if not group_id: return

    # Check agents status once per sweep
    active = get_active_agents_via_clockify()
    if not active: log("   ⚠️ All Agents Offline. Script will Fix & Merge only.")
    
    page = 1
    query = f"(status:2 OR status:3) AND group_id:{group_id}" 
    
    while True:
        try:
            res = requests.get(f"{FD_BASE_URL}/search/tickets?query=\"{query}\"&page={page}", auth=FD_AUTH)
            if check_rate_limit(res): continue
            if res.status_code != 200: break
            tickets = res.json().get('results', [])
            if not tickets: break
            
            log(f"   🔎 Sweeping Batch {page}: Found {len(tickets)} tickets. Adding to Queue...")
            for ticket in tickets:
                TICKET_QUEUE.put(ticket)
            
            if len(tickets) < 30: break 
            page += 1
        except: break
    log("✅ Sweep items queued.")

def background_worker():
    # Start the Queue Worker Thread
    threading.Thread(target=worker, daemon=True).start()
    
    time.sleep(5) 
    run_backlog_sweep()
    while True:
        log("💤 Sweeper sleeping 10 mins...")
        time.sleep(600)
        run_backlog_sweep()

threading.Thread(target=background_worker, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    # Note: On Render this will use their PORT, locally it uses 5000
    log(f"🚀 DISPATCHER STARTED (Port {port})")
    app.run(host='0.0.0.0', port=port)
