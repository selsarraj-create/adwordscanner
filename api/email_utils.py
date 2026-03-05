import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

def send_lead_email(lead_data):
    """
    Send lead notification email via SMTP2GO
    """
    # SMTP Configuration
    smtp_server = "mail-eu.smtp2go.com"
    smtp_port = 2525
    smtp_username = os.getenv('SMTP_USERNAME', 'leadsnyc')
    smtp_password = os.getenv('SMTP_PASSWORD', 'enQ3a3FuMHA1OTAw')
    sender_email = os.getenv('SMTP_SENDER', 'leads@nycscouts.com')
    recipient_email = os.getenv('LEAD_NOTIFICATION_EMAIL', 'asmarketingltd@gmail.com')
    
    # Email subject: [Name] - [CITYCODE]
    # Extract City Code: Remove last 2 chars (Age + Gender suffix) from full campaign code
    # Example: #DALFB33F -> #DALFB3
    full_campaign = lead_data.get('campaign', 'N/A')
    city_code = full_campaign[:-2] if full_campaign and len(full_campaign) > 2 else full_campaign
    
    subject = f"{lead_data.get('first_name', '')} {lead_data.get('last_name', '')} - {city_code}"
    
    # Email body with all lead fields
    body = f"""
New Lead Submission

Name: {lead_data.get('first_name', '')} {lead_data.get('last_name', '')}
Email: {lead_data.get('email', '')}
Phone: {lead_data.get('phone', '')}
Age: {lead_data.get('age', '')}
Gender: {lead_data.get('gender', '')}
City: {lead_data.get('city', '')}
Zip Code: {lead_data.get('zip_code', '')}
Campaign Code: {lead_data.get('campaign', '')}
Score: {lead_data.get('score', '')}
Category: {lead_data.get('category', '')}
Image URL: {lead_data.get('image_url', '')}

Submitted: {lead_data.get('created_at', '')}
"""
    
    try:
        # Create message
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = recipient_email
        message['Subject'] = subject
        message.attach(MIMEText(body, 'plain'))
        
        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Enable TLS
            server.login(smtp_username, smtp_password)
            server.send_message(message)
            
        print(f"Email sent successfully to {recipient_email}")
        return True
        
    except Exception as e:
        print(f"Failed to send email: {str(e)}")
        return False
