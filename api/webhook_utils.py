import requests
import json
import logging

class WebhookResponse:
    """Mock response object for failed requests"""
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text

def send_webhook(url, payload):
    """
    Send a webhook to the CRM.
    Returns the response object or a mock response with error details if failed.
    """
    if not url:
        print("[WEBHOOK] No webhook URL configured")
        return WebhookResponse(0, "No webhook URL configured")
    
    try:
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'ModelScanner/1.0'
        }
        print(f"[WEBHOOK] Sending POST to {url}")
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        print(f"[WEBHOOK] Response: {response.status_code} — {response.text[:300]}")
        return response
    except requests.exceptions.Timeout:
        msg = "Timeout: Request took longer than 10 seconds"
        print(f"[WEBHOOK] TIMEOUT: {msg}")
        return WebhookResponse(0, msg)
    except requests.exceptions.ConnectionError as e:
        msg = f"Connection Error: {str(e)[:500]}"
        print(f"[WEBHOOK] CONNECTION ERROR: {msg}")
        return WebhookResponse(0, msg)
    except requests.exceptions.SSLError as e:
        msg = f"SSL Error: {str(e)[:500]}"
        print(f"[WEBHOOK] SSL ERROR: {msg}")
        return WebhookResponse(0, msg)
    except requests.exceptions.RequestException as e:
        msg = f"Request Error: {str(e)[:500]}"
        print(f"[WEBHOOK] REQUEST ERROR: {msg}")
        return WebhookResponse(0, msg)
    except Exception as e:
        msg = f"Unexpected Error: {type(e).__name__}: {str(e)[:500]}"
        print(f"[WEBHOOK] UNEXPECTED ERROR: {msg}")
        return WebhookResponse(0, msg)

