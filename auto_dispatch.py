import threading
import time
import json
import logging
import os
import re
from flask import Flask
import requests

# ================= CONFIGURATION =================
DOMAIN = os.environ.get("FRESHDESK_DOMAIN")
API_KEY = os.environ.get("FRESHDESK_API_KEY")

# Group ID for "Agents"
TARGET_GROUP_ID = 159000817198

# ID of the Shopify System User (from your previous logs)
SHOPIFY_SENDER_ID = 159009730069

# Emails to IGNORE when scanning body
IGNORE_EMAILS = [
    "actorahelp@gmail.com", "customerservice@actorasupport.com",
    "mailer@shopify.com", "no-reply@shopify.com",
    "notifications@shopify.com", "support@actorasupport.com"
]

# 🚨 LIVE MODE: Set to False to make changes 🚨
DRY_RUN = False
# =================================================

app = Flask(__name__)
BASE_URL = f"https://{DOMAIN}/api/v2"
AUTH = (API_KEY, "X")
HEADERS = {"Content-Type": "application/json"}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def log(msg):
    logging.info(msg)

def check_rate_limit(response):
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        log(f"⚠️ Rate limit hit. Sleeping {retry_after}s...")
        time.sleep(retry_after + 5)
        return True
    return False

# --- HELPER: Find Email in Body ---
def find_best_email(body_text):
    if not body_text: return None
    # Regex for email addresses
    candidates = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', body_text)
    
    for email in candidates:
        email_clean = email.lower().strip()
        if email_clean not in IGNORE_EMAILS:
            return email_clean
    return None

# --- HELPER: Get/Create Contact ID ---
def get_or_create_contact(email):
    # 1. Search
    try:
        res = requests.get(f"{BASE_URL}/contacts?email={email}", auth=AUTH)
        if res.status_code == 200 and len(res.json()) > 0:
            return res.json()[0]['id']
    except: pass

    # 2. Create
    if DRY_RUN: return 999999
    try:
        payload = {"email": email, "name": email.split('@')[0]}
        res = requests.post(f"{BASE_URL}/contacts", auth=AUTH, headers=HEADERS, json=payload)
        if res.status_code == 201: return res.json()['id']
    except: pass
    return None

# --- HELPER: Fix Requester ---
def fix_requester_if_needed(ticket):
    """Checks if ticket is from Shopify and fixes the requester."""
    current_requester_id = ticket.get('requester_id')
    t_id = ticket['id']
    
    if current_requester_id == SHOPIFY_SENDER_ID:
        log(f"🔎 Ticket #{t_id} is from Shopify. Scanning for real customer...")
        
        # We must fetch the full ticket to get the description_text (Search API excludes it)
        try:
            res = requests.get(f"{BASE_URL}/tickets/{t_id}?include=description", auth=AUTH)
            if res.status_code == 200:
                full_ticket = res.json()
                body = full_ticket.get('description_text', '') or full_ticket.get('description', '')
                
                real_email = find_best_email(body)
                if real_email:
                    new_contact_id = get_or_create_contact(real_email)
                    if new_contact_id:
                        if not DRY_RUN:
                            # Update the ticket's requester
                            requests.put(f"{BASE_URL}/tickets/{t_id}", auth=AUTH, headers=HEADERS, 
                                         json={"requester_id": new_contact_id})
                        log(f"✅ Fixed #{t_id}: Requester changed to {real_email}")
                        return True
                    else:
                        log(f"⚠️ Could not create contact for {real_email}")
                else:
                    log(f"⚠️ No email found in body of #{t_id}")
            else:
                log(f"❌ Failed to fetch details for #{t_id}")
        except Exception as e:
            log(f"Error fixing requester: {e}")
            
    return False

def get_available_agents():
    """Fetches 'Available' agents in the target group."""
    available_agents = []
    page = 1
    
    while True:
        url = f"{BASE_URL}/groups/{TARGET_GROUP_ID}/agents?per_page=100&page={page}"
        try:
            res = requests.get(url, auth=AUTH)
            if check_rate_limit(res): continue
            if res.status_code != 200: break
                
            agents = res.json()
            if not agents: break
            
            for agent in agents:
                if agent.get("available", False) == True:
                    available_agents.append(agent['id'])
            page += 1
        except: break
            
    return available_agents

def get_unresolved_tickets():
    """Fetches tickets in group that are Open(2) or Pending(3)."""
    tickets = []
    page = 1
    # We grab tickets in the specific group
    query = f"group_id:{TARGET_GROUP_ID} AND (status:2 OR status:3)"
    
    log("📥 Fetching unresolved tickets...")
    while True:
        url = f"{BASE_URL}/search/tickets?query=\"{query}\"&page={page}"
        try:
            res = requests.get(url, auth=AUTH)
            if check_rate_limit(res): continue
            if res.status_code != 200: break
                
            data = res.json()
            current_batch = data.get('results', [])
            if not current_batch: break
            
            tickets.extend(current_batch)
            if page % 5 == 0: log(f"   Fetched {len(tickets)} tickets...")
            page += 1
            if len(tickets) >= 2000: break 
        except: break
            
    return tickets

def assign_ticket(ticket_id, agent_id):
    url = f"{BASE_URL}/tickets/{ticket_id}"
    payload = {"responder_id": agent_id}
    
    if DRY_RUN:
        log(f"   [DRY RUN] Assign #{ticket_id} -> Agent {agent_id}")
        return True

    try:
        res = requests.put(url, auth=AUTH, headers=HEADERS, data=json.dumps(payload))
        if check_rate_limit(res): return assign_ticket(ticket_id, agent_id)
        if res.status_code == 200:
            log(f"✅ Assigned #{ticket_id} -> Agent {agent_id}")
            return True
        return False
    except: return False

def run_dispatch_cycle():
    log("========================================")
    log("STARTING AUTO-DISPATCHER (FIX + ASSIGN)")
    log("========================================")

    # 1. Get Agents
    active_agents = get_available_agents()
    if not active_agents:
        log("⛔ No active agents. Skipping cycle.")
        return

    # 2. Get Tickets
    tickets = get_unresolved_tickets()
    log(f"📦 Found {len(tickets)} tickets.")
    
    assigned_count = 0
    agent_idx = 0
    
    for ticket in tickets:
        t_id = ticket['id']
        
        # --- STEP A: Fix Shopify Requester First ---
        fix_requester_if_needed(ticket)
        
        # --- STEP B: Check Assignment ---
        current_responder = ticket.get('responder_id')
        should_reassign = False
        
        # If unassigned OR assigned to someone offline
        if current_responder is None:
            should_reassign = True
        elif current_responder not in active_agents:
            should_reassign = True
            
        if should_reassign:
            target_agent = active_agents[agent_idx % len(active_agents)]
            
            if assign_ticket(t_id, target_agent):
                assigned_count += 1
                agent_idx += 1
            
            if not DRY_RUN: time.sleep(0.5)

    log(f"🎉 CYCLE DONE! Actioned {assigned_count} tickets.")

def background_worker():
    while True:
        try:
            run_dispatch_cycle()
            log("Sleeping for 10 minutes...")
            time.sleep(600) 
        except Exception as e:
            log(f"CRITICAL CRASH: {e}")
            time.sleep(60)

threading.Thread(target=background_worker, daemon=True).start()

@app.route('/')
def home():
    return "Auto-Dispatcher Running", 200

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)