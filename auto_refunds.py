import requests
import time
import base64
import json
from datetime import datetime

# ================= CONFIGURATION =================
FRESHDESK_DOMAIN = "https://actorasupport.freshdesk.com"
API_KEY = "nToPJRmvqzHHWJ6pib36"

# The Daily Limit (In USD)
DAILY_LIMIT_USD = 300.0

# Exchange Rates (Base: USD)
EXCHANGE_RATES = {
    "USD": 1.0,
    "EUR": 1.10,
    "GBP": 1.30,
    "CAD": 0.75,
    "AUD": 0.65,
    "JPY": 0.007,
    "PHP": 0.018,
}

# Field Names (From your setup)
REFUND_AMOUNT_FIELD = "cf_refund_amount_value"
REFUND_CURRENCY_FIELD = "cf_refund_currency"

# Status ID: 2 = Open
STATUS_REQUESTING_REFUND = 2 

# =================================================

def get_headers():
    auth_string = f"{API_KEY}:X"
    auth_bytes = auth_string.encode('ascii')
    base64_string = base64.b64encode(auth_bytes).decode('ascii')
    return {
        "Content-Type": "application/json",
        "Authorization": f"Basic {base64_string}"
    }

def handle_rate_limits(response):
    """Checks for 429 Rate Limit errors and pauses if needed."""
    if response.status_code == 429:
        retry_after = int(response.headers.get("Retry-After", 60))
        print(f"⚠️ Rate Limit Hit! Pausing Refund Bot for {retry_after}s...")
        time.sleep(retry_after + 5)
        return True
    return False

def make_request(method, url, json_data=None):
    """Wrapper to make requests with automatic rate limit handling."""
    while True:
        try:
            if method == "GET":
                response = requests.get(url, headers=get_headers())
            elif method == "PUT":
                response = requests.put(url, headers=get_headers(), json=json_data)
            elif method == "POST":
                response = requests.post(url, headers=get_headers(), json=json_data)
            
            # If rate limited, loop and try again after sleeping
            if handle_rate_limits(response):
                continue
            
            return response
        except Exception as e:
            print(f"Request Error: {e}")
            time.sleep(5)
            continue

def convert_to_usd(amount, currency):
    if not currency or currency not in EXCHANGE_RATES:
        return amount 
    return round(amount * EXCHANGE_RATES[currency], 2)

def get_todays_usage():
    total_usd = 0.0
    today_str = datetime.now().strftime('%Y-%m-%d')
    query = f"updated_at:>'{today_str}' AND tag:'Refund_Approved'"
    url = f"{FRESHDESK_DOMAIN}/api/v2/search/tickets?query=\"{query}\""
    
    response = make_request("GET", url)
    if response.status_code != 200:
        print(f"⚠️ API Error reading usage: {response.text}")
        return 0.0
        
    tickets = response.json().get('results', [])
    
    for t in tickets:
        if 'Refund_Approved' in t['tags']:
            custom_fields = t.get('custom_fields') or {}
            amount = custom_fields.get(REFUND_AMOUNT_FIELD)
            currency = custom_fields.get(REFUND_CURRENCY_FIELD)
            
            if amount:
                usd_val = convert_to_usd(float(amount), currency)
                total_usd += usd_val
                
    return total_usd

def process_requests():
    current_total_usd = get_todays_usage()
    remaining_budget = DAILY_LIMIT_USD - current_total_usd
    
    print(f"\n📊 STATUS REPORT")
    print(f"Daily Limit:   ${DAILY_LIMIT_USD:.2f}")
    print(f"Used Today:    ${current_total_usd:.2f}")
    print(f"Remaining:     ${remaining_budget:.2f}")
    
    query = f"\"status:{STATUS_REQUESTING_REFUND}\""
    url = f"{FRESHDESK_DOMAIN}/api/v2/search/tickets?query={query}"
    
    response = make_request("GET", url)
    if response.status_code != 200:
        return

    requests_list = response.json().get('results', [])
    
    if not requests_list:
        print("✅ No pending requests.")
        return

    print(f"🔍 Scanning {len(requests_list)} open tickets...")

    count_processed = 0
    for t in requests_list:
        t_id = t['id']
        tags = t['tags'] or []
        
        # Skip processed tickets
        if "Refund_Approved" in tags or "Limit_Exceeded" in tags:
            continue

        custom_fields = t.get('custom_fields') or {}
        amount_val = custom_fields.get(REFUND_AMOUNT_FIELD)
        currency_val = custom_fields.get(REFUND_CURRENCY_FIELD)
        
        if not amount_val:
            continue
            
        count_processed += 1
        amount_native = float(amount_val)
        currency_code = currency_val if currency_val else "USD"
        amount_usd = convert_to_usd(amount_native, currency_code)
        
        print(f"   👉 Ticket #{t_id}: Requesting {amount_native} {currency_code} (${amount_usd} USD)...", end=" ")
        
        if amount_usd <= remaining_budget:
            print("APPROVED ✅")
            
            new_tags = tags + ["Refund_Approved"]
            
            # Update Ticket
            make_request("PUT", f"{FRESHDESK_DOMAIN}/api/v2/tickets/{t_id}", 
                         {"tags": new_tags, "status": 2, "priority": 1})
            
            # Add Note
            note = f"SYSTEM: Refund APPROVED.\nAmount: {amount_native} {currency_code} (${amount_usd} USD).\nBudget Used: ${current_total_usd + amount_usd:.2f}"
            make_request("POST", f"{FRESHDESK_DOMAIN}/api/v2/tickets/{t_id}/notes", 
                         {"body": note, "private": True})
            
            current_total_usd += amount_usd
            remaining_budget -= amount_usd
            
        else:
            print("PAUSED ⛔ (Over Budget)")
            
            new_tags = tags + ["Limit_Exceeded"]
            
            make_request("PUT", f"{FRESHDESK_DOMAIN}/api/v2/tickets/{t_id}", 
                         {"tags": new_tags, "priority": 4})
            
            note = f"SYSTEM: Refund PAUSED. Daily limit reached.\nRequest: ${amount_usd} USD.\nRemaining: ${remaining_budget} USD."
            make_request("POST", f"{FRESHDESK_DOMAIN}/api/v2/tickets/{t_id}/notes", 
                         {"body": note, "private": True})
        
        # 🛑 PACING: Sleep 2 seconds between refunds to share API limit with Dispatcher
        time.sleep(2.0)

    if count_processed == 0:
        print("   (No new refund requests found)")

if __name__ == "__main__":
    print("🤖 Auto-Refund Bot Started (Shared API Mode)...")
    while True:
        process_requests()
        # Run every 60 seconds
        print("   Sleeping for 60 seconds...")
        time.sleep(60)
