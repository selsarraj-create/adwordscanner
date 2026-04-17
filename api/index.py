from fastapi import FastAPI, File, UploadFile, Form, HTTPException, Request, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import json
import os
import time
from typing import Optional, Union
from pydantic import BaseModel
from supabase import create_client, Client
from dotenv import load_dotenv

# Load valid environment
load_dotenv()

# Fix path for Vercel import resolution
import sys
sys.path.append(os.path.dirname(__file__))

# Import local utils
try:
    from vision_logic import analyze_image
except ImportError as e:
    print(f"Vision Import Error: {e}")
    def analyze_image(img_data, mime_type):
        return {"suitability_score": 70, "market_categorization": "Unknown"}

from webhook_utils import send_webhook
from email_utils import send_lead_email

app = FastAPI()

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helper to get Supabase client
def get_supabase() -> Client:
    url = os.getenv('SUPABASE_URL') or os.getenv('VITE_SUPABASE_URL')
    key = (
        os.getenv('BACKEND_SERVICE_KEY') or
        os.getenv('SUPABASE_SERVICE_ROLE_KEY') or 
        os.getenv('VITE_SUPABASE_ANON_KEY') or 
        os.getenv('NEXT_PUBLIC_SUPABASE_ANON_KEY') or 
        os.getenv('SUPABASE_ANON_KEY') or 
        os.getenv('SUPABASE_PUBLISHABLE_KEY')
    )
    if not url or not key:
        raise HTTPException(status_code=500, detail="Supabase credentials missing")
    return create_client(url, key)

def process_lead_background(lead_record: dict, client_ip: str, user_agent: str):
    """
    Background task to handle Meta CAPI, CRM Webhook, and Emails.
    This runs after the response has been sent to the user.
    """
    print(f"Starting background processing for lead {lead_record.get('id')}")
    
    # 1. Meta Conversion API
    try:
        from api.meta_utils import send_conversion_event
        send_conversion_event(lead_record, client_ip, user_agent)
    except Exception as e:
        print(f"Meta CAPI failed in background: {e}")

    # 2. CRM Webhook
    webhook_url = os.getenv('CRM_WEBHOOK_URL')
    if webhook_url:
        try:
            supabase = get_supabase()
            
            # Prepare CRM payload
            address = f"{lead_record.get('city', '')}, {lead_record.get('zip_code', '')}"
            
            crm_payload = {
                'campaign': lead_record.get('campaign', ''),
                'email': lead_record.get('email'),
                'telephone': lead_record.get('phone'),
                'address': address,
                'firstname': lead_record.get('first_name'),
                'lastname': lead_record.get('last_name'),
                'image': lead_record.get('image_url', ''),
                'analyticsid': '', 
                'age': str(lead_record.get('age', '')),
                'gender': 'M' if lead_record.get('gender') == 'Male' else 'F',
                'opt_in': 'true' if lead_record.get('wants_assessment') else 'false'
            }
            
            import requests
            headers = {
                'Content-Type': 'application/json',
                'User-Agent': 'ModelScanner/1.0'
            }
            
            print(f"Sending background webhook to: {webhook_url}")
            wb_resp = requests.post(webhook_url, json=crm_payload, headers=headers, timeout=10)
            
            status = 'success' if wb_resp.status_code < 300 else 'failed'
            resp_text = wb_resp.text
            
        except Exception as e:
            status = 'failed'
            resp_text = f"Background Error: {str(e)[:200]}"
            
        # Update Webhook Status
        try:
            get_supabase().table('leads').update({
                'webhook_sent': True,
                'webhook_status': status,
                'webhook_response': resp_text
            }).eq('id', lead_record['id']).execute()
        except Exception as e:
            print(f"Failed to update webhook status: {e}")

    # 3. Send Email Notification
    try:
        print("Sending background email notification...")
        send_lead_email(lead_record)
    except Exception as e:
        print(f"Error sending background email: {e}")

@app.post("/api/lead")
async def create_lead(
    background_tasks: BackgroundTasks,  # Injected by FastAPI
    file: Optional[UploadFile] = File(None),
    first_name: str = Form(...),
    last_name: str = Form(...),
     request: Request = None, # Allow request to be optional or explicit
    age: str = Form(...),
    gender: str = Form(...),
    email: str = Form(...),
    phone: str = Form(...),
    city: str = Form(...),
    zip_code: str = Form(...),
    campaign: Optional[str] = Form(None),
    wants_assessment: Optional[str] = Form("false"),
    analysis_data: Optional[str] = Form("{}")
):
    try:
        supabase = get_supabase()
        
        # 1. Duplicate Check
        existing = supabase.table('leads').select('id').or_(f"email.eq.{email},phone.eq.{phone}").execute()
        if existing.data and len(existing.data) > 0:
            return JSONResponse(
                status_code=400,
                content={"status": "error", "message": "This email or phone number has already been submitted."}
            )

        # 2. Image Upload
        image_url = None
        if file:
            # Validate allowed file types
            if file.content_type not in ["image/jpeg", "image/png", "image/jpg"]:
                return JSONResponse(
                    status_code=400,
                    content={"status": "error", "message": "Only JPEG and PNG images are allowed."}
                )

            try:
                content = await file.read()
                timestamp = int(time.time())
                clean_email = email.replace('@', '-at-').replace('.', '-')
                
                # Determine correct extension based on actual file type
                extension = '.jpeg'  # default
                if file.content_type == 'image/png':
                    extension = '.png'
                elif file.content_type in ['image/jpeg', 'image/jpg']:
                    extension = '.jpeg'
                    
                filename = f"{clean_email}_{timestamp}{extension}"
                
                # Upload
                upload_response = supabase.storage.from_("leads").upload(
                    path=filename,
                    file=content,
                    file_options={"content-type": "application/octet-stream"}
                )
                
                sb_url = os.getenv('SUPABASE_URL') or os.getenv('VITE_SUPABASE_URL')
                image_url = f"{sb_url}/storage/v1/object/public/leads/{filename}"
            except Exception as e:
                print(f"Upload failed: {e}")
                return {
                    "status": "error",
                    "message": f"Image upload failed: {str(e)}",
                }

        # 3. Prepare Data
        try:
            analysis_json = json.loads(analysis_data)
        except:
            analysis_json = {}
            
        score = analysis_json.get('suitability_score', 0)
        market_data = analysis_json.get('market_categorization', {})
        category = market_data.get('primary', 'Unknown') if isinstance(market_data, dict) else str(market_data)
        
        # Insert Record
        lead_record = {
            'first_name': first_name,
            'last_name': last_name,
            'age': age,
            'gender': gender,
            'email': email,
            'phone': phone,
            'city': city,
            'zip_code': zip_code,
            'campaign': campaign,
            'wants_assessment': (wants_assessment == 'true'),
            'score': score,
            'category': category,
            'analysis_json': analysis_json,
            'image_url': image_url,
            'webhook_sent': False,
            'webhook_status': 'pending',
            'webhook_response': None
        }
        
        result = supabase.table('leads').insert(lead_record).execute()
        
        if not result.data:
            raise Exception("Insert failed")
            
        final_record = result.data[0]
        
        # --- 4. Queue Background Tasks ---
        # Get client info for Meta
        client_ip = request.client.host if request and request.client else "0.0.0.0"
        user_agent = request.headers.get('user-agent', '') if request else ""
        
        background_tasks.add_task(process_lead_background, final_record, client_ip, user_agent)
            
        return {
            "status": "success",
            "lead_id": final_record['id'],
            "message": "Lead saved successfully."
        }

            
        return {
            "status": "success",
            "lead_id": lead_id,
            "message": "Lead saved successfully."
        }

    except Exception as e:
        print(f"Error: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/analyze")
async def analyze_endpoint(file: UploadFile = File(...)):
    try:
        content = await file.read()
        mime_type = file.content_type or "image/jpeg"
        
        result = analyze_image(content, mime_type=mime_type)
        
        # DOUBLE CHECK: Enforce strict minimum score of 70 at the API level
        # This overrides anything returned by the vision engine
        try:
            current_score = int(result.get('suitability_score', 0))
            result['suitability_score'] = max(current_score, 70)
        except:
            result['suitability_score'] = 70
            
        return result
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# --- Campaign Code Recalculation (mirrors frontend LeadForm.jsx logic) ---
import math
import requests as http_requests

TARGET_CITIES = {
    'Boston': {'code': '#BOSYT', 'lat': 42.3601, 'lon': -71.0589},
    'New York': {'code': '#NY3CX', 'lat': 40.7128, 'lon': -74.0060},
    'Dallas': {'code': '#DAL3CX', 'lat': 32.7767, 'lon': -96.7970},
    'Houston': {'code': '#HOU3CX', 'lat': 29.7604, 'lon': -95.3698},
    'Nashville': {'code': '#NA3CX', 'lat': 36.1627, 'lon': -86.7816},
    'Miami': {'code': '#FL3CX', 'lat': 25.7617, 'lon': -80.1918},
    'Chicago': {'code': '#CHI3CX', 'lat': 41.8781, 'lon': -87.6298},
    'Orlando': {'code': '#ORL3CX', 'lat': 28.5383, 'lon': -81.3792},
}
BOSTON_STATES = {'CT', 'MA', 'NH', 'RI'}

def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def recalculate_campaign_code(zip_code, age, gender):
    """Recalculate campaign code server-side using the same logic as the frontend."""
    try:
        resp = http_requests.get(f"https://api.zippopotam.us/us/{zip_code}", timeout=5)
        if resp.status_code == 200:
            place = resp.json()['places'][0]
            user_lat = float(place['latitude'])
            user_lon = float(place['longitude'])
            state = place.get('state abbreviation', '')

            # Block unsupported states
            blocked_states = {
                'WA', 'OR', 'CA', 'NV', 'AZ', 'UT', 'ID', 'MT', 'WY', 'CO',
                'NM', 'ND', 'SD', 'NE', 'KS', 'OK', 'AR', 'LA', 'MO', 'MS',
                'AL', 'GA', 'SC', 'NC', 'VA', 'ME', 'AK', 'HI'
            }
            if state in blocked_states:
                print(f"[CAMPAIGN] Blocked state: {state}")
                return '#BLOCKED'

            if state in BOSTON_STATES:
                city_code = '#BOSYT'
            else:
                nearest = None
                min_dist = float('inf')
                for city_data in TARGET_CITIES.values():
                    d = _haversine_km(user_lat, user_lon, city_data['lat'], city_data['lon'])
                    if d < min_dist:
                        min_dist = d
                        nearest = city_data['code']
                city_code = nearest or '#NY3CX'

            # Boston: flat code, no suffix
            if city_code == '#BOSYT':
                return city_code

            # Gender code
            gender_code = 'F' if gender == 'Female' else 'M'

            # Age code
            try:
                age_num = int(age)
            except (ValueError, TypeError):
                age_num = 25

            # Florida: custom age buckets
            if city_code == '#FL3CX':
                if gender_code == 'M':
                    return f"{city_code}1M"
                else:
                    fl_age_code = '2' if age_num >= 35 else '1'
                    return f"{city_code}{fl_age_code}F"

            # Default age codes for all other cities
            age_code = '1'
            if 35 <= age_num <= 44:
                age_code = '2'
            elif age_num >= 45:
                age_code = '3'

            return f"{city_code}{age_code}{gender_code}"
    except Exception as e:
        print(f"[CAMPAIGN] Recalculation failed: {e}")

    # Fallback: return a default
    return '#NY3CX1M'

class RetryRequest(BaseModel):
    lead_id: Union[int, str]

@app.post("/api/retry_webhook")
async def retry_webhook(req: RetryRequest):
    print(f"[RETRY_WEBHOOK] Starting retry for lead_id={req.lead_id}")
    try:
        supabase = get_supabase()
        webhook_url = os.getenv('CRM_WEBHOOK_URL')
        
        if not webhook_url:
            print("[RETRY_WEBHOOK] ERROR: CRM_WEBHOOK_URL not configured")
            raise HTTPException(status_code=400, detail="CRM_WEBHOOK_URL not configured")
        
        print(f"[RETRY_WEBHOOK] Webhook URL: {webhook_url[:30]}...")
            
        resp = supabase.table('leads').select('*').eq('id', req.lead_id).execute()
        if not resp.data:
            print(f"[RETRY_WEBHOOK] ERROR: Lead {req.lead_id} not found in database")
            raise HTTPException(status_code=404, detail="Lead not found")
             
        lead_record = resp.data[0]
        print(f"[RETRY_WEBHOOK] Found lead: {lead_record.get('first_name')} {lead_record.get('last_name')} ({lead_record.get('email')})")
        
        # Recalculate campaign code using current logic
        campaign = recalculate_campaign_code(
            lead_record.get('zip_code', ''),
            lead_record.get('age', ''),
            lead_record.get('gender', '')
        )
        print(f"[RETRY_WEBHOOK] Recalculated campaign: {campaign} (was: {lead_record.get('campaign', '')})")
        
        # Build CRM payload with recalculated campaign
        address = f"{lead_record.get('city', '')}, {lead_record.get('zip_code', '')}"
        crm_payload = {
            'campaign': campaign,
            'email': lead_record.get('email'),
            'telephone': lead_record.get('phone'),
            'address': address,
            'firstname': lead_record.get('first_name'),
            'lastname': lead_record.get('last_name'),
            'image': lead_record.get('image_url', ''),
            'analyticsid': '',
            'age': str(lead_record.get('age', '')),
            'gender': 'M' if lead_record.get('gender') == 'Male' else 'F',
            'opt_in': 'true' if lead_record.get('wants_assessment') else 'false'
        }
        print(f"[RETRY_WEBHOOK] CRM payload telephone: {crm_payload['telephone']}")
        
        wb_resp = send_webhook(webhook_url, crm_payload)
        
        status = 'success' if wb_resp is not None and wb_resp.status_code < 300 else 'failed'
        resp_text = wb_resp.text if wb_resp is not None else "Connection failed"
        
        print(f"[RETRY_WEBHOOK] Webhook response: status_code={wb_resp.status_code if wb_resp is not None else 'None'}, result={status}")
        print(f"[RETRY_WEBHOOK] Response body: {resp_text[:500]}")
        
        # Update webhook status AND the corrected campaign code in the database
        supabase.table('leads').update({
            'campaign': campaign,
            'webhook_sent': True,
            'webhook_status': status,
            'webhook_response': resp_text
        }).eq('id', req.lead_id).execute()
        
        print(f"[RETRY_WEBHOOK] Database updated for lead {req.lead_id}, status={status}, campaign={campaign}")
        
        return {
            "status": "success", 
            "message": "Webhook retry attempted",
            "webhook_status": status,
            "webhook_response": resp_text[:200]
        }
    except HTTPException:
        raise
    except Exception as e:
        print(f"[RETRY_WEBHOOK] EXCEPTION for lead {req.lead_id}: {type(e).__name__}: {str(e)}")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/test_webhook")
async def test_webhook_connection():
    """
    Test endpoint to verify CRM webhook connectivity from Vercel's serverless environment.
    Returns detailed diagnostic information about the connection attempt.
    """
    import time
    import requests
    
    webhook_url = os.getenv('CRM_WEBHOOK_URL')
    
    if not webhook_url:
        return {
            "status": "error",
            "message": "CRM_WEBHOOK_URL not configured"
        }
    
    # Test payload
    test_payload = {
        'campaign': '#TEST1M',
        'email': 'test@vercel-test.com',
        'telephone': '1234567890',
        'address': 'Test City, 12345',
        'firstname': 'Vercel',
        'lastname': 'Test',
        'image': '',
        'analyticsid': '',
        'age': '25',
        'gender': 'M',
        'opt_in': 'false'
    }
    
    start_time = time.time()
    error_details = None
    response_data = None
    status_code = None
    
    try:
        headers = {
            'Content-Type': 'application/json',
            'User-Agent': 'ModelScanner-Test/1.0'
        }
        response = requests.post(webhook_url, json=test_payload, headers=headers, timeout=10)
        elapsed_time = time.time() - start_time
        status_code = response.status_code
        response_data = response.text[:500]  # Limit response size
        
        return {
            "status": "success" if status_code < 300 else "failed",
            "webhook_url": webhook_url,
            "status_code": status_code,
            "response_time_seconds": round(elapsed_time, 2),
            "response_preview": response_data,
            "message": "Connection successful" if status_code < 300 else f"HTTP {status_code} error"
        }
        
    except requests.exceptions.Timeout:
        elapsed_time = time.time() - start_time
        error_details = f"Timeout after {round(elapsed_time, 2)} seconds"
    except requests.exceptions.ConnectionError as e:
        error_details = f"Connection Error: {str(e)[:300]}"
    except requests.exceptions.SSLError as e:
        error_details = f"SSL Error: {str(e)[:300]}"
    except requests.exceptions.RequestException as e:
        error_details = f"Request Error: {str(e)[:300]}"
    except Exception as e:
        error_details = f"Unexpected Error: {str(e)[:300]}"
    
    return {
        "status": "error",
        "webhook_url": webhook_url,
        "error": error_details,
        "message": "Failed to connect to CRM server from Vercel"
    }
