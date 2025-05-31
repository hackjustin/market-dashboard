#!/usr/bin/env python3
"""Simple email test script for Market Dashboard"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv

def test_email_config():
    """Test email configuration from .env file"""
    
    load_dotenv()
    
    smtp_server = os.environ.get('SMTP_SERVER')
    smtp_port = int(os.environ.get('SMTP_PORT', 587))
    username = os.environ.get('SMTP_USERNAME')
    password = os.environ.get('SMTP_PASSWORD')
    alert_email = os.environ.get('ALERT_EMAIL')
    
    missing = []
    if not smtp_server: missing.append('SMTP_SERVER')
    if not username: missing.append('SMTP_USERNAME')
    if not password: missing.append('SMTP_PASSWORD')
    if not alert_email: missing.append('ALERT_EMAIL')
    
    if missing:
        print(f"❌ Missing configuration: {', '.join(missing)}")
        return False
    
    print("📧 Testing email configuration...")
    print(f"   Server: {smtp_server}:{smtp_port}")
    print(f"   From: {username}")
    print(f"   To: {alert_email}")
    
    try:
        msg = MIMEMultipart()
        msg['From'] = username
        msg['To'] = alert_email
        msg['Subject'] = "Market Dashboard - Email Test"
        
        body = f"""
This is a test email from your Market Dashboard application.

If you're receiving this, your email configuration is working correctly!

Configuration details:
- SMTP Server: {smtp_server}:{smtp_port}
- From: {username}
- To: {alert_email}

You can now enable email alerts in your dashboard.
        """
        
        msg.attach(MIMEText(body, 'plain'))
        
        print("🔗 Connecting to SMTP server...")
        server = smtplib.SMTP(smtp_server, smtp_port)
        
        print("🔐 Starting TLS encryption...")
        server.starttls()
        
        print("🔑 Authenticating...")
        server.login(username, password)
        
        print("📤 Sending test email...")
        server.sendmail(username, alert_email, msg.as_string())
        
        print("🔌 Closing connection...")
        server.quit()
        
        print("✅ Email sent successfully!")
        print(f"📬 Check {alert_email} for the test message")
        return True
        
    except Exception as e:
        print(f"❌ Email test failed: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Market Dashboard Email Test")
    print("=" * 40)
    
    if not os.path.exists('.env'):
        print("❌ .env file not found!")
        print("   Run ./setup-env.sh first")
        exit(1)
    
    test_email_config()
