import google.generativeai as genai
import os
import json
import typing_extensions as typing
from dotenv import load_dotenv

load_dotenv()

# Fallback or load from environment
# In a real scenario, ensure GOOGLE_API_KEY is in .env or passed here
API_KEY = os.getenv("GOOGLE_API_KEY")
API_KEY = os.getenv("GOOGLE_API_KEY")
if not API_KEY:
    # Allow running without key if just testing scaffolding, but warn.
    print("WARNING: GOOGLE_API_KEY not found in environment.")

genai.configure(api_key=API_KEY)

# Define the response schema explicitly for Gemini 1.5 strict output
class FaceGeometry(typing.TypedDict):
    primary_shape: str
    jawline_definition: str
    structural_note: str

class MarketCategorization(typing.TypedDict):
    primary: str
    rationale: str

class AestheticAudit(typing.TypedDict):
    lighting_quality: str
    professional_readiness: str
    technical_flaw: str

class AnalysisResult(typing.TypedDict):
    face_geometry: FaceGeometry
    market_categorization: MarketCategorization
    aesthetic_audit: AestheticAudit
    suitability_score: int
    scout_feedback: str

from google.generativeai.types import HarmCategory, HarmBlockThreshold

# Config for balanced creativity and JSON format
generation_config = {
    "temperature": 0.4,
    "response_mime_type": "application/json",
    "response_schema": AnalysisResult
}

# Safety settings to allow model analysis (BLOCK_NONE)
safety_settings = {
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
}

model = genai.GenerativeModel(
    'gemini-3-flash-preview',
    generation_config=generation_config,
    safety_settings=safety_settings
)

def analyze_image(image_bytes, mime_type="image/jpeg"):
    """
    Analyzes an image using Gemini 1.5 Flash to extract technical industry markers.
    """
    try:
        # Prompt Pivot: Professional Technical Audit
        prompt = """
        ACT AS: A Senior Global Model Scout for a top-tier agency (e.g., IMG, Elite). 

        TASK: Perform a high-fidelity structural audit of the provided image.

        REASONING STEPS (Internal Process):
        1. Observe the lighting: Identify shadows, light source, and skin texture clarity.
        2. Map facial geometry: Identify the 3 most dominant bone structure markers.
        3. Categorize: Cross-reference findings against 2026 fashion industry standards.

        OUTPUT DATA (JSON Format only):
        {
          "face_geometry": {
            "primary_shape": "[Heart, Square, Oval, Round, Diamond, Oblong, Triangular]",
            "jawline_definition": "[Soft, Sharp, Chiseled, Defined, Angular]",
            "structural_note": "Technical observation of cheekbone height and symmetry."
          },
          "market_categorization": {
            "primary": "[High Fashion, Commercial/Lifestyle, Fitness]",
            "rationale": "Why does this face fit this specific market?"
          },
          "aesthetic_audit": {
            "lighting_quality": "[Natural, Studio, Poor, Harsh]",
            "professional_readiness": "[Selfie, Amateur, Semi-Pro, Portfolio-Ready]",
            "technical_flaw": "Specific issue like 'motion blur', 'under-eye shadows', or 'distorting lens angle'."
          },
          "suitability_score": "Integer 70-100. Score based on natural modeling potential.",
          "scout_feedback": "A professional, direct 1-sentence assessment of the model's market potential."
        }

        CONSTRAINTS: 
        - Analyze ALL fields with precision. Every field must be filled.
        - Be thorough about facial geometry analysis (jawline, face shape, structure).
        - For suitability_score: Focus on natural features (bone structure, proportions). Most people score 75-85. Exceptional candidates 90+.
        - Use precise industry terminology (e.g., 'high-fashion edge', 'relatable commercial appeal').
        - Return ONLY valid JSON.
        """
        
        # Validating input type
        if not image_bytes:
            raise ValueError("No image data provided")
        
        # Ensure image_bytes is passed correctly
        # The SDK handles bytes directly if passed as a Part with mime_type
        response = model.generate_content(
            [
                {"mime_type": mime_type, "data": image_bytes}, 
                prompt
            ]
        )
        
        # Check validation
        print(f"Candidates generated: {len(response.candidates)}")
        if not response.parts:
             # If blocked despite safety settings, log it
             print(f"Prompt FeedBack: {response.prompt_feedback}")
             
        result = json.loads(response.text)
        
        # Enforce minimum score of 70 as requested
        if 'suitability_score' in result:
            try:
                score = int(result['suitability_score'])
                print(f"Raw Score: {score}")
                result['suitability_score'] = max(score, 70)
            except:
                result['suitability_score'] = 70
        else:
            result['suitability_score'] = 70
        
        # Add fallback values for fields that AI sometimes skips
        if 'face_geometry' in result:
            if not result['face_geometry'].get('jawline_definition'):
                result['face_geometry']['jawline_definition'] = 'Defined'
        
        if not result.get('scout_feedback'):
            result['scout_feedback'] = 'Strong commercial potential with natural appeal.'
                
        return result

    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Error in Gemini analysis: {e}")
        # Return a mock response if API fails (for development safety) or re-raise
        # For now, returning minimal error structure
        return {
            "error": f"{str(e)}",
            "suitability_score": 70,
            "market_categorization": {"primary": "Unknown", "rationale": "Analysis failed."},
            "face_geometry": {"primary_shape": "Unknown", "jawline_definition": "Unknown", "structural_note": "N/A"},
            "aesthetic_audit": {"lighting_quality": "Unknown", "professional_readiness": "Unknown", "technical_flaw": "Analysis Error"},
            "scout_feedback": f"Analysis failed: {str(e)}"
        }
