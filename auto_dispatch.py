import threading
import time
import json
import logging
import os
import re
from flask import Flask, request
import requests

# ================= CONFIGURATION =================
# Freshdesk Config
FD_DOMAIN = os.environ.get("FRESHDESK_DOMAIN")
FD_API_KEY = os.environ.get("FRESHDESK_API_KEY")

# Clockify Config (ADD THESE TO RENDER ENV VARS)
CLOCK_API_KEY = os.environ.get("CLOCKIFY_API_KEY")
# If you don't know your Workspace ID, the script will find it automatically.
CLOCK_WORKSPACE_ID = os.environ.get("CLOCKIFY_WORKSPACE_ID") 

# The 3 Agents we trust (Jean, Lance, Vanesa)
AGENT_IDS = [
    159009628844, 
    159009628874, 
    159009628889
]

TARGET_GROUP_NAME = "Agents" 
SHOPIFY_SENDER_ID = 159009730069
IGNORE_EMAILS = ["actorahelp@gmail.com", "customerservice@actorasupport.com", "mailer@shopify.com", "no-reply@shopify.com", "notifications@shopify.com", "support@actorasupport.com"]

# 🚨 LIVE MODE 🚨
DRY_RUN = False  
# =================================================

app = Flask(__name__)
FD_BASE_URL = f"https://{FD_DOMAIN}/api/v2"
FD_AUTH = (FD_API_KEY, "X")
FD_HEADERS = {"Content-Type": "application/json"}
CLOCK_HEADERS = {"X-Api-Key": CLOCK_API_KEY}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
def log(msg): logging.info(msg)

# Global Cache
CACHED_GROUP_ID = None
CACHED_WORKSPACE_ID = CLOCK_WORKSPACE_ID
CACHED_CLOCKIFY_USERS = {} # Map email -> Clockify User ID

def check_rate_limit(response):
    if response.status_code == 429:
        retry = int(response.headers.get("Retry-After", 60))
        log(f"⚠️ Rate limit hit. Sleeping {retry}s...")
        time.sleep(retry + 5)
        return True
    return False

# --- CLOCKIFY LOGIC ---
def get_clockify_workspace():
    global CACHED_WORKSPACE_ID
    if CACHED_WORKSPACE_ID: return CACHED_WORKSPACE_ID
    
    try:
        res = requests.get("https://api.clockify.me/api/v1/workspaces", headers=CLOCK_HEADERS)
        if res.status_code == 200:
            workspaces = res.json()
            if workspaces:
                # Default to the first workspace found
                CACHED_WORKSPACE_ID = workspaces[0]['id']
                log(f"   🕒 Found Clockify Workspace: {workspaces[0]['name']}")
                return CACHED_WORKSPACE_ID
    except Exception as e: log(f"Clockify Workspace Error: {e}")
    return None

def get_clockify_user_id(email):
    global CACHED_CLOCKIFY_USERS
    if email in CACHED_CLOCKIFY_USERS: return CACHED_CLOCKIFY_USERS[email]
    
    ws_id = get_clockify_workspace()
    if not ws_id: return None
    
    try:
        # Fetch all users in workspace
        res = requests.get(f"https://api.clockify.me/api/v1/workspaces/{ws_id}/users", headers=CLOCK_HEADERS)
        if res.status_code == 200:
            users = res.json()
            for u in users:
                # Cache everyone found
                CACHED_CLOCKIFY_USERS[u['email']] = u['id']
            
            return CACHED_CLOCKIFY_USERS.get(email)
    except: pass
    return None

def is_user_clocked_in(email):
    ws_id = get_clockify_workspace()
    user_id = get_clockify_user_id(email)
    
    if not ws_id or not user_id: return False
    
    try:
        # Check for currently running timer
        url = f"https://api.clockify.me/api/v1/workspaces/{ws_id}/user/{user_id}/time-entries?in-progress=true"
        res = requests.get(url, headers=CLOCK_HEADERS)
        if res.status_code == 200:
            entries = res.json()
            # If list is not empty, they have a running timer
            return len(entries) > 0
    except: pass
    return False

def get_active_agents_via_clockify():
    """
    Checks our 3 agents. Instead of asking Freshdesk if they are 'Available',
    we ask Clockify if they are 'Clocked In'.
    """
    active_list = []
    
    for agent_id in AGENT_IDS:
        # 1. Get Agent Email from Freshdesk
        try:
            res = requests.get(f"{FD_BASE_URL}/agents/{agent_id}", auth=FD_AUTH)
            if res.status_code == 200:
                agent_data = res.json()
                email = agent_data['contact']['email']
                name = agent_data['contact']['name']
                
                # 2. Check Clockify Status
                if is_user_clocked_in(email):
                    # log(f"   🕒 {name} is CLOCKED IN.")
                    active_list.append(agent_id)
                else:
                    # log(f"   zzz {name} is Clocked Out.")
                    pass
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
        res = requests.get(f"{FD_BASE_URL}/search/tickets?query=\"{query}\"", auth=FD_AUTH)
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

    # 3. ASSIGN (Via Clockify Status)
    active_agents = get_active_agents_via_clockify()
    group_id = get_group_id()
    
    current_responder = ticket_object.get('responder_id')
    should_assign = False
    
    # Logic: Assign if Unassigned OR if currently assigned agent is Clocked Out
    if current_responder is None: 
        should_assign = True
    elif current_responder in AGENT_IDS and current_responder not in active_agents:
        # Reassign if they clocked out
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
        else:
             # log(f"   ⚠️ No agents clocked in. Ticket #{t_id} waiting.")
             pass

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
    log("🧹 STARTING BACKLOG SWEEP (Checking Clockify Status)...")
    group_id = get_group_id()
    if not group_id: return

    # Check who is working right now
    active = get_active_agents_via_clockify()
    if not active:
        log("   💤 Everyone is clocked out. Sweep skipped (Merging only).")

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
                # If active agents exist, pass None to force re-check inside (slower) or pass list (faster)
                # Let's re-check inside process_single_ticket so we have freshest data
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
    return "Auto-Dispatcher (CLOCKIFY MODE) Running", 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
