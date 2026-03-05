import requests
import json
import logging

def send_webhook(url, payload):
    """
    Send a webhook to the CRM.
    Returns the response object or None if failed.
    """
    if not url:
        return None
    
    try:
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'ModelScanner/1.0'
        }
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        return response
    except Exception as e:
        print(f"Webhook error: {str(e)}")
        # In a real app we might want to log this properly
        return None
