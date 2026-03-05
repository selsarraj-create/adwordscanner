from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import shutil
import os
from vision_engine import analyze_image
import database

app = FastAPI()

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allow all for dev, restrict in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize DB
database.init_db()

class Lead(BaseModel):
    first_name: str
    last_name: str
    age: int
    gender: str
    email: str
    phone: str
    city: str
    zip_code: str
    wants_assessment: bool
    analysis_data: dict

@app.get("/")
def read_root():
    return {"message": "Model Suitability Scanner API is running"}

@app.post("/analyze")
async def analyze_endpoint(file: UploadFile = File(...)):
    if not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="File must be an image")
    
    try:
        content = await file.read()
        # Call Vision Engine
        result = analyze_image(content, mime_type=file.content_type)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/lead")
def submit_lead(lead: Lead):
    try:
        lead_id = database.save_lead(lead)
        # Mock Email Sending
        print(f"Sending email to {lead.email} with report...")
        return {"status": "success", "lead_id": lead_id, "message": "Lead saved and report sent (mocked)."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
