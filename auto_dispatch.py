import threading
import time
import json
import logging
import os
import re
import queue
from datetime import datetime, timedelta, timezone
from flask import Flask, request
import requests

# ================= PRODUCTION CONFIGURATION =================
# 1. Credentials (Load from Render Environment)
FD_DOMAIN = os.environ.get("FRESHDESK_DOMAIN")
FD_API_KEY = os.environ.get("FRESHDESK_API_KEY")
CLOCK_API_KEY = os.environ.get("CLOCKIFY_API_KEY")

# 2. Agent Management
ENV_AGENT_LIST = os.environ.get("AGENT_IDS")
AGENT_IDS = []
if ENV_AGENT_LIST:
    try:
        AGENT_IDS = [int(x.strip()) for x in ENV_AGENT_LIST.split(',') if x.strip().isdigit()]
    except: pass

SHOPIFY_SENDER_ID = int(os.environ.get("SHOPIFY_SENDER_ID", "0"))

IGNORE_EMAILS = [
    "actorahelp@gmail.com", "customerservice@actorasupport.com", 
    "mailer@shopify.com", "no-reply@shopify.com", 
    "notifications@shopify.com", "support@actorasupport.com"
]

# PERFORMANCE SETTINGS (SAFE FOR 200 REQ/MIN)
# 2 Workers x 5s delay = ~24 tickets/min.
# Max 3 API calls per ticket = ~72 req/min (Very Safe)
NUM_WORKER_THREADS = 2   
WORKER_DELAY = 5.0       
CLOCKIFY_CACHE_SECONDS = 60
DRY_RUN = False  
# ============================================================

app = Flask(__name__)
FD_BASE_URL = f"https://{FD_DOMAIN}/api/v2"
FD_AUTH = (FD_API_KEY, "X")
FD_HEADERS = {"Content-Type": "application/json"}
CLOCK_HEADERS = {"X-Api-Key": CLOCK_API_KEY}

# GLOBAL STATE
TICKET_QUEUE = queue.Queue()
RATE_LIMIT_UNTIL = 0
RATE_LOCK = threading.Lock()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
def log(msg): logging.info(msg)

CACHED_WORKSPACE_ID = None
CACHED_CLOCKIFY_USERS = {} 
STATUS_CACHE = {} 

# --- RATE LIMIT HANDLER ---
def handle_rate_limits(response):
    global RATE_LIMIT_UNTIL
    if response.status_code == 429:
        with RATE_LOCK:
            if time.time() < RATE_LIMIT_UNTIL: return True
            retry = int(response.headers.get("Retry-After", 60))
            RATE_LIMIT_UNTIL = time.time() + retry + 10
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
            log(f"   👤 Clockify Connected: {user_data.get('name')}")
            return True
    except Exception as e: log(f"Clockify Error: {e}")
    return False

def build_clockify_cache():
    global CACHED_CLOCKIFY_USERS
    CACHED_CLOCKIFY_USERS = {}
    if not CACHED_WORKSPACE_ID:
        if not init_clockify(): return

    wait_if_limited()
    try:
        res = requests.get(f"https://api.clockify.me/api/v1/workspaces/{CACHED_WORKSPACE_ID}/users", headers=CLOCK_HEADERS)
        if res.status_code == 200:
            for u in res.json():
                CACHED_CLOCKIFY_USERS[u['email'].lower()] = u['id']
    except: pass

def is_user_clocked_in(email):
    email = email.lower()
    
    if email in STATUS_CACHE:
        data = STATUS_CACHE[email]
        if time.time() - data['last_check'] < CLOCKIFY_CACHE_SECONDS:
            return data['is_online']

    if not CACHED_CLOCKIFY_USERS: build_clockify_cache()
    if email not in CACHED_CLOCKIFY_USERS: return False
    
    ws_id = CACHED_WORKSPACE_ID
    user_id = CACHED_CLOCKIFY_USERS[email]
    is_online = False
    
    wait_if_limited()
    try:
        url = f"https://api.clockify.me/api/v1/workspaces/{ws_id}/user/{user_id}/time-entries?in-progress=true"
        res = requests.get(url, headers=CLOCK_HEADERS)
        if res.status_code == 200:
            entries = res.json()
            if len(entries) > 0:
                is_online = True
    except: pass
    
    STATUS_CACHE[email] = { "is_online": is_online, "last_check": time.time() }
    return is_online

def get_active_agents_via_clockify():
    active_list = []
    if not CACHED_CLOCKIFY_USERS: build_clockify_cache()
    
    for agent_id in AGENT_IDS:
        wait_if_limited()
        try:
            res = requests.get(f"{FD_BASE_URL}/agents/{agent_id}", auth=FD_AUTH)
            if handle_rate_limits(res): continue
            
            if res.status_code == 200:
                primary_email = res.json()['contact']['email']
                is_active = is_user_clocked_in(primary_email)
                
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

# --- LOGIC PIPELINE ---

def fix_requester_if_needed(ticket):
    """
    CRITICAL: We still check this so your External Merger 
    can match the correct email!
    """
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

def assign_to_agent(t_id, current_responder, status_label):
    active_agents = get_active_agents_via_clockify()
    target_responder = None
    
    if current_responder in active_agents:
        target_responder = current_responder
    else:
        if active_agents:
            import random
            target_responder = random.choice(active_agents)
        else:
            target_responder = current_responder 

    if target_responder and target_responder != current_responder:
        if not DRY_RUN:
            wait_if_limited()
            res = requests.put(f"{FD_BASE_URL}/tickets/{t_id}", auth=FD_AUTH, headers=FD_HEADERS, json={"responder_id": target_responder})
            if not handle_rate_limits(res) and res.status_code == 200:
                log(f"   👮 Assigned #{t_id} -> Agent {target_responder} ({status_label})")

def unassign_ticket(t_id, current_responder, status_label):
    if current_responder is not None:
        if not DRY_RUN:
            wait_if_limited()
            res = requests.put(f"{FD_BASE_URL}/tickets/{t_id}", auth=FD_AUTH, headers=FD_HEADERS, json={"responder_id": None})
            if not handle_rate_limits(res) and res.status_code == 200:
                log(f"   ⏸️ Unassigned #{t_id} ({status_label})")

def manage_assignment(ticket):
    t_id = ticket['id']
    status = ticket['status']
    current_responder = ticket.get('responder_id')
    
    # 4=Resolved: ALWAYS UNASSIGN
    if status == 4:
        unassign_ticket(t_id, current_responder, "Resolved")
        return

    # 2=Open: ALWAYS ASSIGN
    if status == 2:
        assign_to_agent(t_id, current_responder, "Open")
        return

    # 3=Pending: CHECK 24H RULE
    if status == 3:
        try:
            updated_str = ticket.get('updated_at')
            if updated_str:
                if updated_str.endswith('Z'):
                    updated_at = datetime.strptime(updated_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
                else:
                    updated_at = datetime.fromisoformat(updated_str).astimezone(timezone.utc)
                
                time_diff = datetime.now(timezone.utc) - updated_at
                hours_passed = time_diff.total_seconds() / 3600
                
                if hours_passed > 24:
                    assign_to_agent(t_id, current_responder, "Pending > 24h")
                else:
                    unassign_ticket(t_id, current_responder, "Pending < 24h")
        except Exception as e:
            log(f"   ⚠️ Date Error #{t_id}: {e}")

def process_single_ticket(ticket_data):
    wait_if_limited()
    t_id = ticket_data['id']
    
    try:
        res = requests.get(f"{FD_BASE_URL}/tickets/{t_id}", auth=FD_AUTH)
        if not handle_rate_limits(res) and res.status_code == 200:
            full_ticket = res.json()
            
            # 1. Fix Email (So external merger works)
            fix_requester_if_needed(full_ticket)
            
            # 2. Dispatch (No merging here)
            manage_assignment(full_ticket)
            
    except Exception as e: log(f"Error: {e}")

# --- WORKER ---
def worker(name):
    log(f"🔧 Worker {name} started.")
    while True:
        ticket_data = TICKET_QUEUE.get()
        try:
            process_single_ticket(ticket_data)
        except Exception as e: log(f"Worker Error: {e}")
        finally:
            TICKET_QUEUE.task_done()
            time.sleep(WORKER_DELAY)

# --- WEBHOOK ---
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.json
    t_id = data.get('ticket_id')
    if t_id:
        TICKET_QUEUE.put({'id': t_id, 'requester_id': data.get('requester_id')})
    return "Queued", 200

@app.route('/', methods=['GET', 'HEAD'])
def health(): return "OK", 200

# --- SWEEPER ---
def run_backlog_sweep():
    log("🧹 STARTING BACKLOG SWEEP...")
    wait_if_limited()
    page = 1
    query = "status:2 OR status:3 OR status:4" 
    
    while True:
        try:
            res = requests.get(f"{FD_BASE_URL}/search/tickets?query=\"{query}\"&page={page}", auth=FD_AUTH)
            if handle_rate_limits(res): break
            if res.status_code != 200: 
                log(f"❌ Sweep Error: {res.status_code} - {res.text}")
                break
            
            tickets = res.json().get('results', [])
            if not tickets: break
            
            log(f"   🔎 Batch {page}: Queuing {len(tickets)} tickets...")
            for ticket in tickets:
                TICKET_QUEUE.put(ticket)
            
            if len(tickets) < 30: break 
            
            if page >= 10:
                log("🛑 Max Search Depth (300 tickets). Restarting soon...")
                break
            page += 1
        except: break
    log("✅ Sweep items queued.")

def background_worker():
    for i in range(NUM_WORKER_THREADS):
        threading.Thread(target=worker, args=(i,), daemon=True).start()
    
    time.sleep(5) 
    run_backlog_sweep()
    while True:
        time.sleep(900)
        run_backlog_sweep()

threading.Thread(target=background_worker, daemon=True).start()

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    log(f"🚀 LIGHTWEIGHT DISPATCHER (NO MERGE) STARTED (Port {port})")
    app.run(host='0.0.0.0', port=port)
