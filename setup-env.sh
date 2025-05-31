#!/bin/bash

echo "🔧 Market Dashboard Environment Setup"
echo "====================================="

# Generate secure secret key
SECRET_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(32))' 2>/dev/null || openssl rand -hex 32)

# Check if .env already exists
if [ -f ".env" ]; then
    echo "⚠ .env file already exists!"
    read -p "Do you want to overwrite it? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Setup cancelled."
        exit 0
    fi
    mv .env .env.backup.$(date +%Y%m%d_%H%M%S)
fi

echo "ℹ Creating .env file..."

cat > .env << EOL
# Flask Configuration
SECRET_KEY=${SECRET_KEY}
FLASK_ENV=production

# Database Configuration
EOL

echo
echo "Database Configuration"
echo "======================"

read -p "Database host [localhost]: " db_host
db_host=${db_host:-localhost}
echo "DB_HOST=${db_host}" >> .env

read -p "Database port [5432]: " db_port
db_port=${db_port:-5432}
echo "DB_PORT=${db_port}" >> .env

read -p "Database name [market_data]: " db_name
db_name=${db_name:-market_data}
echo "DB_NAME=${db_name}" >> .env

read -p "Database user [postgres]: " db_user
db_user=${db_user:-postgres}
echo "DB_USER=${db_user}" >> .env

echo -n "Database password: "
read -s db_password
echo
echo "DB_PASSWORD=${db_password}" >> .env

cat >> .env << EOL

# Application Configuration
LOG_LEVEL=INFO
DATA_REFRESH_INTERVAL=5
MARKET_SCORE_THRESHOLD_BULLISH=70
MARKET_SCORE_THRESHOLD_BEARISH=30

# Security Configuration
MAX_CONNECTIONS=20
CONNECTION_TIMEOUT=30

# Feature Flags
ENABLE_EMAIL_ALERTS=false
ENABLE_HISTORICAL_CHARTS=true
ENABLE_ADVANCED_ANALYTICS=true
EOL

echo
read -p "Do you want to configure email alerts? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Email Alert Configuration"
    echo "========================="
    
    read -p "SMTP Server (e.g., smtp.gmail.com): " smtp_server
    read -p "SMTP Port [587]: " smtp_port
    smtp_port=${smtp_port:-587}
    read -p "Email username: " smtp_username
    echo -n "Email password/app password: "
    read -s smtp_password
    echo
    read -p "Alert recipient email: " alert_email
    
    cat >> .env << EOL

# Email Configuration
SMTP_SERVER=${smtp_server}
SMTP_PORT=${smtp_port}
SMTP_USERNAME=${smtp_username}
SMTP_PASSWORD=${smtp_password}
ALERT_EMAIL=${alert_email}
EOL

    sed -i 's/ENABLE_EMAIL_ALERTS=false/ENABLE_EMAIL_ALERTS=true/' .env
fi

chmod 600 .env
echo "✓ .env file created successfully!"
echo "✓ Secure permissions set on .env file"

echo
echo "Testing database connection..."

python3 << 'PYEOF'
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

try:
    conn = psycopg2.connect(
        host=os.environ.get('DB_HOST'),
        port=os.environ.get('DB_PORT'),
        database=os.environ.get('DB_NAME'),
        user=os.environ.get('DB_USER'),
        password=os.environ.get('DB_PASSWORD')
    )
    conn.close()
    print("✓ Database connection successful!")
except Exception as e:
    print(f"✗ Database connection failed: {e}")
    print("Please check your database configuration.")
PYEOF

echo
echo "✓ Environment setup complete!"
echo
echo "Next steps:"
echo "1. Start the application: docker-compose up -d"
echo "2. View logs: docker-compose logs -f"
echo "3. Open dashboard: http://localhost:5150"