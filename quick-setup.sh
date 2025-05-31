#!/bin/bash

# Quick Setup Script - Creates all necessary files for Market Dashboard
# Run this first, then run setup-env.sh

set -e

echo "🚀 Creating Market Dashboard Project Files"
echo "=========================================="

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

# Create templates directory
mkdir -p templates
mkdir -p logs

# Create requirements.txt
print_info "Creating requirements.txt..."
cat > requirements.txt << 'EOF'
Flask==2.3.3
yfinance==0.2.22
pandas==2.1.1
numpy==1.24.3
APScheduler==3.10.4
requests==2.31.0
Werkzeug==2.3.7
psycopg2-binary==2.9.7
python-dotenv==1.0.0
EOF
print_success "requirements.txt created"

# Create Dockerfile
print_info "Creating Dockerfile..."
cat > Dockerfile << 'EOF'
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create templates directory
RUN mkdir -p templates logs

# Expose port
EXPOSE 5150

# Set environment variables
ENV FLASK_APP=app.py
ENV FLASK_ENV=production
ENV PYTHONUNBUFFERED=1

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:5150/api/data || exit 1

# Run the application
CMD ["python", "app.py"]
EOF
print_success "Dockerfile created"

# Create docker-compose.yml
print_info "Creating docker-compose.yml..."
cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  market-dashboard:
    build: .
    container_name: market-dashboard
    ports:
      - "5150:5150"
    env_file:
      - .env
    volumes:
      - ./logs:/app/logs
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:5150/api/data"]
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
    networks:
      - market-network
    # Use host networking to connect to existing PostgreSQL on host
    extra_hosts:
      - "host.docker.internal:host-gateway"

  # Optional: Include PostgreSQL if you want it containerized instead
  # Comment out this service if using existing PostgreSQL on host
  postgres:
    image: postgres:15-alpine
    container_name: market-postgres
    env_file:
      - .env
    environment:
      - POSTGRES_DB=${DB_NAME}
      - POSTGRES_USER=${DB_USER}
      - POSTGRES_PASSWORD=${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "${DB_PORT:-5432}:5432"
    restart: unless-stopped
    networks:
      - market-network

  # Optional: Database admin interface
  pgadmin:
    image: dpage/pgadmin4:latest
    container_name: market-pgadmin
    environment:
      - PGADMIN_DEFAULT_EMAIL=${PGADMIN_EMAIL:-admin@example.com}
      - PGADMIN_DEFAULT_PASSWORD=${PGADMIN_PASSWORD:-admin}
    ports:
      - "8080:80"
    restart: unless-stopped
    networks:
      - market-network
    depends_on:
      - postgres

networks:
  market-network:
    driver: bridge

volumes:
  postgres_data:
  logs:
EOF
print_success "docker-compose.yml created"

# Create .gitignore
print_info "Creating .gitignore..."
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Virtual environment
venv/
env/
ENV/

# Flask
instance/
.webassets-cache

# Environment variables - IMPORTANT: Keep .env files secret!
.env
.env.local
.env.development.local
.env.test.local
.env.production.local
.env.backup*

# Logs
logs/
*.log

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Docker
.dockerignore

# Database backups
*.sql
*.dump
EOF
print_success ".gitignore created"

# Create .env.example
print_info "Creating .env.example..."
cat > .env.example << 'EOF'
# Flask Configuration
SECRET_KEY=your-super-secret-flask-key-change-this-to-something-random
FLASK_ENV=production

# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=market_data
DB_USER=postgres
DB_PASSWORD=your-postgres-password

# Optional: Email Configuration for Alerts
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USERNAME=your-email@gmail.com
SMTP_PASSWORD=your-app-password
ALERT_EMAIL=alerts@yourdomain.com

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

# Optional: PgAdmin Configuration
PGADMIN_EMAIL=admin@example.com
PGADMIN_PASSWORD=admin
EOF
print_success ".env.example created"

echo
print_success "All project files created!"
echo
echo "📋 Next steps:"
echo "1. Copy and customize the main application files:"
echo "   - Copy app.py (Enhanced Flask application)"
echo "   - Copy templates/dashboard.html (Dashboard interface)"
echo "   - Copy setup-env.sh (Environment setup script)"
echo "   - Copy test-email.py (Email testing script)"
echo
echo "2. Run the environment setup:"
echo "   chmod +x setup-env.sh"
echo "   ./setup-env.sh"
echo
echo "3. Build and start the application:"
echo "   docker-compose up -d"
echo
echo "4. View the dashboard:"
echo "   http://localhost:5150"

print_info "Remember to copy the remaining files from the artifacts before proceeding!"
EOF

print_success "quick-setup.sh created and ready to run!"
