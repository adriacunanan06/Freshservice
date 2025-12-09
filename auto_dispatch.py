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

# ================= PRODUCTION CONFIGURATION =================
FD_DOMAIN = os.environ.get("FRESHDESK_DOMAIN")
FD_API_KEY = os.environ.get("FRESHDESK_API_KEY")
CLOCK_API_KEY = os.environ.get("CLOCKIFY_API_KEY")

# AGENT LIST (Cyril, Jean, Lance, Vanesa)
AGENT_IDS = [
    159009628895, # Cyril Poche
    159009628844, # Jean Kreanne Dawatan
    159009628874, # Lance Anthony
    159009628889  # Vanesa Joy Roble
]

SHOPIFY_SENDER_ID = 159009730069

IGNORE_EMAILS = [
    "actorahelp@gmail.com", "customerservice@actorasupport.com", 
    "mailer@shopify.com", "no-reply@shopify.com", 
    "notifications@shopify.com", "support@actorasupport.com"
]

DRY_RUN = False  
# ============================================================

app = Flask(__name__)
FD_BASE_URL = f"https://{FD_DOMAIN}/api/v2"
FD_AUTH = (FD_API_KEY, "X")
FD_HEADERS = {"Content-Type": "application/json"}
CLOCK_HEADERS = {"X-Api-Key": CLOCK_API_KEY}

# GLOBAL QUEUE
TICKET_QUEUE = queue.Queue()
RATE_LIMIT_UNTIL = 0

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
def log(msg): logging.info(msg)

CACHED_WORKSPACE_ID = None
CACHED_CLOCKIFY_MAP = {} 

# --- RATE LIMIT HANDLER ---
def handle_rate_limits(response):
    global RATE_LIMIT_UNTIL
    if response.status_code == 429:
        retry = int(response.headers.get("Retry-After", 60))
        RATE_LIMIT_UNTIL = time.time() + retry + 5
        log(f"⚠️ RATE LIMIT HIT! Pausing for {retry}s...")
        return True
    return False

def wait_if_limited():
    remaining = RATE_LIMIT_UNTIL - time.time()
    if remaining > 0: time.sleep(remaining)

# --- CLOCKIFY LOGIC ---
def init_clockify():
    global CACHED_WORKSPACE_ID
    if not CLOCK_API_KEY:
        log("❌ Clockify Key Missing!")
        return False

    wait_if_limited()
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

    wait_if_limited()
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
    
    wait_if_limited()
    try:
        url = f"https://api.clockify.me/api/v1/workspaces/{ws_id}/user/{user_id}/time-entries?in-progress=true"
        res = requests.get(url, headers=CLOCK_HEADERS)
        if res.status_code == 200:
            entries = res.json()
            if len(entries) > 0:
                return True
    except: pass
    return False

def get_active_agents_via_clockify():
    active_list = []
    if not CACHED_CLOCKIFY_MAP: build_clockify_cache()
    
    for agent_id in AGENT_IDS:
        wait_if_limited()
        try:
            res = requests.get(f"{FD_BASE_URL}/agents/{agent_id}", auth=FD_AUTH)
            if handle_rate_limits(res): continue
            
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
def find_best_email(body_text):
    if not body_text: return None
    candidates = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', body_text)
    for email in candidates:
        if email.lower().strip() not in IGNORE_EMAILS: return email.lower().strip()
    return None

def get_or_create_contact(email):
    wait_if_limited()
    try:
        res = requests.get(f"{FD_BASE_URL}/contacts?email={email}", auth=FD_AUTH)
        if handle_rate_limits(res): return None
        if res.status_code == 200 and len(res.json()) > 0: return res.json()[0]['id']
        
        if not DRY_RUN:
            wait_if_limited()
            res = requests.post(f"{FD_BASE_URL}/contacts", auth=FD_AUTH, headers=FD_HEADERS, json={"email": email, "name": email.split('@')[0]})
            if handle_rate_limits(res): return None
            if res.status_code == 201: return res.json()['id']
    except: pass
    return None

def merge_tickets(primary_id, secondary_ids):
    if DRY_RUN or not secondary_ids: return False
    url = f"{FD_BASE_URL}/tickets/merge"
    payload = { "primary_id": primary_id, "ticket_ids": secondary_ids }
    
    wait_if_limited()
    try:
        res = requests.put(url, auth=FD_AUTH, headers=FD_HEADERS, data=json.dumps(payload))
        if handle_rate_limits(res): return False
        
        if res.status_code in [200, 204]:
            log(f"   ⚡ Merged {len(secondary_ids)} tickets into #{primary_id}")
            return True
    except: pass
    return False

# --- LOGIC PIPELINE ---

def fix_requester_if_needed(ticket):
    """Step 1: Fix Shopify Emails"""
    tid = ticket['id']
    req_id = ticket['requester_id']
    
    if req_id == SHOPIFY_SENDER_ID:
        wait_if_limited()
        try:
            res = requests.get(f"{FD_BASE_URL}/tickets/{tid}?include=description", auth=FD_AUTH)
            if handle_rate_limits(res): return req_id
            
            if res.status_code == 200:
                body = res.json().get('description_text', '')
                real_email = find_best_email(body)
                if real_email:
                    new_cid = get_or_create_contact(real_email)
                    if new_cid and not DRY_RUN:
                        wait_if_limited()
                        requests.put(f"{FD_BASE_URL}/tickets/{tid}", auth=FD_AUTH, headers=FD_HEADERS, json={"requester_id": new_cid})
                        log(f"   ✅ Fixed #{tid} Requester -> {real_email}")
                        return new_cid
        except: pass
    return req_id

def perform_merge_check(t_id, requester_id, ticket_object):
    """Step 2: Check for Duplicates and Merge"""
    query = f"requester_id:{requester_id} AND (status:2 OR status:3)"
    wait_if_limited()
    try:
        res = requests.get(f"{FD_BASE_URL}/search/tickets?query=\"{query}\"", auth=FD_AUTH)
        if handle_rate_limits(res): return t_id
        
        if res.status_code == 200:
            user_tickets = res.json().get('results', [])
            ids = [t['id'] for t in user_tickets]
            if t_id not in ids: user_tickets.append(ticket_object)

            if len(user_tickets) > 1:
                user_tickets.sort(key=lambda x: x['created_at'])
                primary = user_tickets[-1] 
                secondary = [t['id'] for t in user_tickets if t['id'] != primary['id']]
                
                log(f"   🔄 Merging duplicates for #{primary['id']}...")
                if merge_tickets(primary['id'], secondary):
                    if t_id in secondary: return None # Current ticket deleted
                    else: return primary['id'] # Return surviving ID
    except: pass
    return t_id

def assign_active_agent(t_id, current_responder_id):
    """Step 3: Assign to Clocked-In Agent"""
    active_agents = get_active_agents_via_clockify()
    target_responder = None
    
    # CASE 1: Keep current agent if they are ONLINE
    if current_responder_id in active_agents:
        target_responder = current_responder_id
        
    # CASE 2: If Unassigned OR Assigned to OFFLINE -> Reassign
    else:
        if active_agents:
            import random
            target_responder = random.choice(active_agents)
        else:
            # Everyone is offline. Keep current owner if exists, else leave unassigned.
            target_responder = current_responder_id 

    # Only update if it changed
    if target_responder and target_responder != current_responder_id:
        if not DRY_RUN:
            wait_if_limited()
            payload = {"responder_id": target_responder}
            res = requests.put(f"{FD_BASE_URL}/tickets/{t_id}", auth=FD_AUTH, headers=FD_HEADERS, json=payload)
            
            if handle_rate_limits(res): return
            if res.status_code == 200:
                log(f"   👮 Assigned #{t_id} -> Agent {target_responder}")

def process_single_ticket(ticket_data):
    wait_if_limited()
    t_id = ticket_data['id']
    log(f"⚡ Processing #{t_id}...")
    
    # Fetch full ticket to get current status
    try:
        res = requests.get(f"{FD_BASE_URL}/tickets/{t_id}", auth=FD_AUTH)
        if handle_rate_limits(res): return
        if res.status_code == 200:
            full_ticket = res.json()
            
            # 1. Fix Requester
            real_req_id = fix_requester_if_needed(full_ticket)
            
            # 2. Merge Duplicates
            surviving_id = perform_merge_check(t_id, real_req_id, full_ticket)
            
            # 3. Assign Agent (If ticket survived merge)
            if surviving_id:
                # If we merged, we might need to re-fetch the survivor's status?
                # Usually fine to just proceed if it was the primary.
                # If t_id changed to primary, we use that ID.
                
                # Re-fetch survivor to be safe about responder status
                res_s = requests.get(f"{FD_BASE_URL}/tickets/{surviving_id}", auth=FD_AUTH)
                if res_s.status_code == 200:
                    survivor_ticket = res_s.json()
                    assign_active_agent(surviving_id, survivor_ticket.get('responder_id'))

    except Exception as e: log(f"Process Error: {e}")

# --- WORKER THREAD ---
def worker():
    while True:
        ticket_data = TICKET_QUEUE.get()
        try:
            process_single_ticket(ticket_data)
        except Exception as e:
            log(f"Worker Error: {e}")
        finally:
            TICKET_QUEUE.task_done()
            time.sleep(10) # 10s delay to protect API limits

# --- WEBHOOK ---
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    t_id = data.get('ticket_id')
    if t_id:
        TICKET_QUEUE.put({'id': t_id, 'requester_id': data.get('requester_id')})
    return "Queued", 200

# --- HEALTH CHECK ---
@app.route('/', methods=['GET', 'HEAD'])
def health_check():
    return "Healthy", 200

# --- SWEEPER ---
def run_backlog_sweep():
    # log("🧹 STARTING SWEEP...")
    wait_if_limited()
    
    page = 1
    query = "status:2 OR status:3" 
    
    while True:
        try:
            res = requests.get(f"{FD_BASE_URL}/search/tickets?query=\"{query}\"&page={page}", auth=FD_AUTH)
            if handle_rate_limits(res): break
            if res.status_code != 200: break
            
            tickets = res.json().get('results', [])
            if not tickets: break
            
            log(f"   🔎 Sweeper Batch {page}: Queuing {len(tickets)} tickets...")
            for ticket in tickets:
                TICKET_QUEUE.put(ticket)
            
            if len(tickets) < 30: break 
            page += 1
        except: break

def background_worker():
    threading.Thread(target=worker, daemon=True).start()
    time.sleep(10) 
    run_backlog_sweep()
    while True:
        time.sleep(900)
        run_backlog_sweep()

threading.Thread(target=background_worker, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
