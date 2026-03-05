import os
from vision_engine import analyze_image

# Read real image from disk
image_path = "d:/Gemini_Generated_Image_mer7p4mer7p4mer7.PNG"
if not os.path.exists(image_path):
    print(f"Error: {image_path} not found.")
    exit(1)

with open(image_path, "rb") as f:
    real_image_bytes = f.read()

print(f"Starting analysis test with real image ({len(real_image_bytes)} bytes)...")
try:
    # Use real image bytes and correct mime type (PNG)
    result = analyze_image(real_image_bytes, mime_type="image/png")
    print("Result:", result)
except Exception as e:
    import traceback
    traceback.print_exc()
    print("Error:", e)
