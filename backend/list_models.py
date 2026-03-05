import google.generativeai as genai
import os
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("GOOGLE_API_KEY")
genai.configure(api_key=API_KEY)

print("Listing supported models for generateContent...")
for m in genai.list_models():
    if 'generateContent' in m.supported_generation_methods:
        print(m.name)
