import hashlib
import time
import requests
import os
import json

def hash_data(data):
    """SHA-256 hash for user data (email, phone, etc.)."""
    if not data:
        return None
    return hashlib.sha256(data.strip().lower().encode('utf-8')).hexdigest()

def send_conversion_event(lead_data, client_ip, user_agent):
    """
    Sends a 'Lead' event to Meta Conversion API.
    """
    access_token = os.getenv('META_ACCESS_TOKEN')
    pixel_id = os.getenv('META_PIXEL_ID')
    
    if not access_token or not pixel_id:
        print("Meta CAPI Error: Missing META_ACCESS_TOKEN or META_PIXEL_ID")
        return None

    url = f"https://graph.facebook.com/v19.0/{pixel_id}/events"
    
    # Hash User Data
    user_data = {
        "em": [hash_data(lead_data.get('email'))],
        "ph": [hash_data(lead_data.get('phone'))],
        "fn": [hash_data(lead_data.get('first_name'))],
        "ln": [hash_data(lead_data.get('last_name'))],
        "ct": [hash_data(lead_data.get('city'))],
        "zp": [hash_data(lead_data.get('zip_code'))],
        "client_ip_address": client_ip,
        "client_user_agent": user_agent,
    }
    
    # Construct Payload
    payload = {
        "data": [
            {
                "event_name": "Lead",
                "event_time": int(time.time()),
                "action_source": "website",
                "user_data": user_data,
                "custom_data": {
                    "currency": "USD",
                    "value": 0, # Or estimated lead value
                    "content_name": lead_data.get('campaign', 'Organic'),
                    "suitability_score": lead_data.get('score', 0)
                }
            }
        ],
        "access_token": access_token
    }

    # Add Test Event Code if available (for "Test Events" in Events Manager)
    test_code = os.getenv('META_TEST_CODE') or os.getenv('VITE_META_TEST_CODE')
    if test_code:
        payload['test_event_code'] = test_code
    
    try:
        response = requests.post(url, json=payload)
        response_data = response.json()
        
        if response.status_code == 200:
            print(f"Meta CAPI Success: {response_data}")
            return response_data
        else:
            print(f"Meta CAPI Failed: {response_data}")
            return None
    except Exception as e:
        print(f"Meta CAPI Exception: {e}")
        return None
