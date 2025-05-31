import requests  # Keep for general use
from curl_cffi import requests as curl_requests  # Specific for Yahoo Finance
from flask import Flask, render_template, jsonify, request, redirect, url_for, flash, session
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash
import json
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import numpy as np
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import os
import psycopg2
from psycopg2.extras import RealDictCursor
import psycopg2.pool
from contextlib import contextmanager
from dotenv import load_dotenv
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from trend_engine import ConsensusTrendEngine
import pytz

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Flask configuration from environment
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY')
if not app.config['SECRET_KEY']:
    raise ValueError("SECRET_KEY environment variable is required")

# Initialize Flask-Login
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = 'Please log in to access the dashboard.'
login_manager.login_message_category = 'info'

# Database configuration from environment
DB_CONFIG = {
    'host': os.environ.get('DB_HOST'),
    'port': int(os.environ.get('DB_PORT', 5432)),
    'database': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD')
}

# Validate required database config
required_db_vars = ['DB_HOST', 'DB_NAME', 'DB_USER', 'DB_PASSWORD']
missing_vars = [var for var in required_db_vars if not os.environ.get(var)]
if missing_vars:
    raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

# Application configuration from environment
APP_CONFIG = {
    'log_level': os.environ.get('LOG_LEVEL', 'INFO'),
    'refresh_interval': int(os.environ.get('DATA_REFRESH_INTERVAL', 5)),
    'max_connections': int(os.environ.get('MAX_CONNECTIONS', 20)),
    'connection_timeout': int(os.environ.get('CONNECTION_TIMEOUT', 30)),
    'bullish_threshold': float(os.environ.get('MARKET_SCORE_THRESHOLD_BULLISH', 70)),
    'bearish_threshold': float(os.environ.get('MARKET_SCORE_THRESHOLD_BEARISH', 30)),
    'enable_alerts': os.environ.get('ENABLE_EMAIL_ALERTS', 'false').lower() == 'true',
    'enable_charts': os.environ.get('ENABLE_HISTORICAL_CHARTS', 'true').lower() == 'true',
    'enable_analytics': os.environ.get('ENABLE_ADVANCED_ANALYTICS', 'true').lower() == 'true',
    'backfill_days': int(os.environ.get('BACKFILL_DAYS', 730))
}

# Email configuration (optional)
EMAIL_CONFIG = {
    'smtp_server': os.environ.get('SMTP_SERVER'),
    'smtp_port': int(os.environ.get('SMTP_PORT', 587)),
    'username': os.environ.get('SMTP_USERNAME'),
    'password': os.environ.get('SMTP_PASSWORD'),
    'alert_email': os.environ.get('ALERT_EMAIL')
} if os.environ.get('SMTP_SERVER') else None

# Configure logging from environment
log_level = getattr(logging, APP_CONFIG['log_level'].upper(), logging.INFO)
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/market_dashboard.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database connection pool
connection_pool = None

# User class for Flask-Login
class User(UserMixin):
    def __init__(self, id, email, name, created_at=None):
        self.id = id
        self.email = email
        self.name = name
        self.created_at = created_at

@login_manager.user_loader
def load_user(user_id):
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute('SELECT * FROM users WHERE id = %s', (user_id,))
                user = cur.fetchone()
                if user:
                    return User(user['id'], user['email'], user['name'], user['created_at'])
    except Exception as e:
        logger.error(f"Error loading user: {e}")
    return None

def init_db_pool():
    """Initialize database connection pool"""
    global connection_pool
    try:
        connection_pool = psycopg2.pool.SimpleConnectionPool(
            1, APP_CONFIG['max_connections'],
            connect_timeout=APP_CONFIG['connection_timeout'],
            **DB_CONFIG
        )
        logger.info("Database connection pool created successfully")
        logger.info(f"Connected to database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        return True
    except Exception as e:
        logger.error(f"Failed to create database pool: {e}")
        return False

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    connection = None
    try:
        connection = connection_pool.getconn()
        yield connection
    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"Database error: {e}")
        raise
    finally:
        if connection:
            connection_pool.putconn(connection)

def create_users_table():
    """Create users table for authentication"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS users (
                        id SERIAL PRIMARY KEY,
                        email VARCHAR(255) UNIQUE NOT NULL,
                        name VARCHAR(255) NOT NULL,
                        password_hash VARCHAR(255) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        last_login TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE
                    );
                    CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
                ''')
                conn.commit()
                logger.info("Users table ready")
    except Exception as e:
        logger.error(f"Error creating users table: {e}")

def create_user_tickers_table():
    """Create user ticker preferences table"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS user_tickers (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        ticker_type VARCHAR(10) NOT NULL,
                        symbol VARCHAR(10) NOT NULL,
                        added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_id, symbol)
                    );
                    CREATE INDEX IF NOT EXISTS idx_user_tickers_user_id ON user_tickers(user_id);
                ''')
                conn.commit()
                logger.info("User tickers table ready")
    except Exception as e:
        logger.error(f"Error creating user_tickers table: {e}")

def create_tables():
    """Create database tables if they don't exist"""
    tables = {
        'market_indices': '''
            CREATE TABLE IF NOT EXISTS market_indices (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                symbol VARCHAR(10) NOT NULL,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(12, 4),
                change_pct DECIMAL(8, 4),
                volume BIGINT,
                high_52w DECIMAL(12, 4),
                low_52w DECIMAL(12, 4)
            );
            CREATE INDEX IF NOT EXISTS idx_indices_symbol_time ON market_indices(symbol, timestamp);
        ''',
        'market_breadth': '''
            CREATE TABLE IF NOT EXISTS market_breadth (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                advancing INTEGER,
                declining INTEGER,
                unchanged INTEGER,
                advance_decline_ratio DECIMAL(8, 4),
                new_highs INTEGER,
                new_lows INTEGER,
                new_hl_ratio DECIMAL(8, 4)
            );
            CREATE INDEX IF NOT EXISTS idx_breadth_time ON market_breadth(timestamp);
        ''',
        'sectors': '''
            CREATE TABLE IF NOT EXISTS sectors (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                symbol VARCHAR(10) NOT NULL,
                name VARCHAR(100) NOT NULL,
                price DECIMAL(12, 4),
                change_pct DECIMAL(8, 4)
            );
            CREATE INDEX IF NOT EXISTS idx_sectors_symbol_time ON sectors(symbol, timestamp);
        ''',
        'market_summary': '''
            CREATE TABLE IF NOT EXISTS market_summary (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                overall_trend VARCHAR(20),
                participation VARCHAR(20),
                momentum VARCHAR(20),
                positive_sectors INTEGER,
                total_sectors INTEGER,
                market_score DECIMAL(5, 2)
            );
            CREATE INDEX IF NOT EXISTS idx_summary_time ON market_summary(timestamp);
        '''
    }
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for table_name, query in tables.items():
                    cur.execute(query)
                    logger.info(f"Table {table_name} ready")
                conn.commit()
    except Exception as e:
        logger.error(f"Failed to create tables: {e}")

def create_trend_table():
    """Create trend analysis table"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS trend_analysis (
                        id SERIAL PRIMARY KEY,
                        symbol VARCHAR(10) NOT NULL,
                        trend_score DECIMAL(5,2),
                        trend_rating VARCHAR(20),
                        risk_level VARCHAR(15),
                        ma_alignment DECIMAL(5,2),
                        volume_strength DECIMAL(5,2),
                        momentum DECIMAL(5,2),
                        relative_strength DECIMAL(5,2),
                        stage_analysis DECIMAL(5,2),
                        short_term_trend VARCHAR(10),
                        medium_term_trend VARCHAR(10),
                        long_term_trend VARCHAR(10),
                        alignment_status VARCHAR(30),
                        signals TEXT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(symbol)
                    )
                ''')
                conn.commit()
                logger.info("Table trend_analysis ready")
    except Exception as e:
        logger.error(f"Error creating trend_analysis table: {e}")

def create_breadth_tables():
    """Create tables for market breadth calculation"""
    tables = {
        'index_constituents': '''
            CREATE TABLE IF NOT EXISTS index_constituents (
                id SERIAL PRIMARY KEY,
                index_name VARCHAR(20) NOT NULL,
                symbol VARCHAR(10) NOT NULL,
                company_name VARCHAR(200),
                sector VARCHAR(50),
                industry VARCHAR(100),
                added_date DATE DEFAULT CURRENT_DATE,
                removed_date DATE,
                is_active BOOLEAN DEFAULT TRUE,
                UNIQUE(index_name, symbol)
            );
            CREATE INDEX IF NOT EXISTS idx_constituents_index_active ON index_constituents(index_name, is_active);
            CREATE INDEX IF NOT EXISTS idx_constituents_symbol ON index_constituents(symbol);
        ''',
        'daily_prices': '''
            CREATE TABLE IF NOT EXISTS daily_prices (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                date DATE NOT NULL,
                open DECIMAL(12,4),
                high DECIMAL(12,4),
                low DECIMAL(12,4),
                close DECIMAL(12,4),
                volume BIGINT,
                adj_close DECIMAL(12,4),
                is_final BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, date)
            );
            CREATE INDEX IF NOT EXISTS idx_daily_prices_symbol_date ON daily_prices(symbol, date);
            CREATE INDEX IF NOT EXISTS idx_daily_prices_date ON daily_prices(date);
            CREATE INDEX IF NOT EXISTS idx_daily_prices_symbol ON daily_prices(symbol);
        ''',
        'daily_breadth': '''
            CREATE TABLE IF NOT EXISTS daily_breadth (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL,
                index_name VARCHAR(20) NOT NULL,
                advancing INTEGER DEFAULT 0,
                declining INTEGER DEFAULT 0,
                unchanged INTEGER DEFAULT 0,
                new_highs INTEGER DEFAULT 0,
                new_lows INTEGER DEFAULT 0,
                ad_ratio DECIMAL(8,4),
                hl_ratio DECIMAL(8,4),
                total_issues INTEGER,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(date, index_name)
            );
            CREATE INDEX IF NOT EXISTS idx_daily_breadth_date ON daily_breadth(date);
            CREATE INDEX IF NOT EXISTS idx_daily_breadth_index ON daily_breadth(index_name);
        ''',
        'breadth_summary': '''
            CREATE TABLE IF NOT EXISTS breadth_summary (
                id SERIAL PRIMARY KEY,
                date DATE NOT NULL UNIQUE,
                combined_advancing INTEGER DEFAULT 0,
                combined_declining INTEGER DEFAULT 0,
                combined_unchanged INTEGER DEFAULT 0,
                combined_new_highs INTEGER DEFAULT 0,
                combined_new_lows INTEGER DEFAULT 0,
                combined_ad_ratio DECIMAL(8,4),
                combined_hl_ratio DECIMAL(8,4),
                market_breadth_score DECIMAL(5,2),
                breadth_rating VARCHAR(20),
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_breadth_summary_date ON breadth_summary(date);
        '''
    }
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for table_name, query in tables.items():
                    cur.execute(query)
                    logger.info(f"Table {table_name} ready")
                conn.commit()
                logger.info("✅ All breadth tables created successfully")
    except Exception as e:
        logger.error(f"Failed to create breadth tables: {e}")

def get_chrome_session():
    return curl_requests.Session(impersonate="chrome")

def is_market_open():
    """Check if US stock market is currently open"""
    try:
        et = pytz.timezone('US/Eastern')
        now_et = datetime.now(et)
        
        market_open_time = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close_time = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
        
        is_weekday = now_et.weekday() < 5
        is_open = (is_weekday and market_open_time <= now_et <= market_close_time)
        
        if is_open:
            next_open = now_et
            status = "MARKET OPEN"
        elif is_weekday and now_et < market_open_time:
            next_open = market_open_time
            status = "PRE-MARKET"
        elif is_weekday and now_et > market_close_time:
            tomorrow = now_et + timedelta(days=1)
            next_open = tomorrow.replace(hour=9, minute=30, second=0, microsecond=0)
            status = "AFTER-HOURS"
        else:
            days_ahead = 7 - now_et.weekday()
            if now_et.weekday() == 6:
                days_ahead = 1
            elif now_et.weekday() == 5:
                days_ahead = 2
            
            next_monday = now_et + timedelta(days=days_ahead)
            next_open = next_monday.replace(hour=9, minute=30, second=0, microsecond=0)
            status = "MARKET CLOSED - WEEKEND"
        
        return is_open, next_open, status
        
    except Exception as e:
        logger.error(f"Error checking market hours: {e}")
        return False, None, "UNKNOWN"

def validate_ticker_symbol(symbol):
    """Validate if a ticker symbol exists and is tradeable"""
    try:
        symbol = symbol.upper().strip()
        
        if not symbol or len(symbol) < 1 or len(symbol) > 5:
            return False, {'error': 'Invalid ticker format'}
        
        session = get_chrome_session()
        ticker = yf.Ticker(symbol, session=session)
        hist = ticker.history(period="5d")
        info = ticker.info
        
        if hist.empty:
            return False, {'error': f'No trading data found for {symbol}'}
        
        if 'longName' in info or 'shortName' in info:
            return True, {
                'symbol': symbol,
                'name': info.get('longName', info.get('shortName', symbol)),
                'sector': info.get('sector', 'Unknown'),
                'last_price': float(hist['Close'].iloc[-1]) if not hist.empty else None
            }
        else:
            return True, {
                'symbol': symbol,
                'name': symbol,
                'sector': 'Unknown',
                'last_price': float(hist['Close'].iloc[-1]) if not hist.empty else None
            }
            
    except Exception as e:
        return False, {'error': f'Error validating {symbol}: {str(e)}'}

def backfill_user_ticker_on_demand(symbol):
    """Auto-backfill ticker data when missing"""
    try:
        logger.info(f"🔄 Auto-backfilling {symbol}...")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT COUNT(*) FROM daily_prices WHERE symbol = %s', (symbol,))
                existing_count = cur.fetchone()[0]
        
        if existing_count > 50:
            logger.info(f"✅ {symbol}: Sufficient data exists ({existing_count} records)")
            return True
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=300)
        
        session = get_chrome_session()
        ticker = yf.Ticker(symbol, session=session)
        hist = ticker.history(start=start_date, end=end_date)
        
        if hist.empty:
            logger.warning(f"❌ {symbol}: No historical data available")
            return False
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                records_added = 0
                for date, row in hist.iterrows():
                    cur.execute('''
                        INSERT INTO daily_prices 
                        (symbol, date, open, high, low, close, volume, adj_close, is_final)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (symbol, date) DO NOTHING
                    ''', (
                        symbol, date.date(),
                        float(row['Open']), float(row['High']), 
                        float(row['Low']), float(row['Close']),
                        int(row['Volume']), float(row['Close']),
                        True
                    ))
                    records_added += 1
                conn.commit()
        
        logger.info(f"✅ {symbol}: Backfilled {records_added} records")
        return True
        
    except Exception as e:
        logger.error(f"Error backfilling {symbol}: {e}")
        return False

def fetch_and_store_trend_data():
    """Fetch trend analysis and store in database"""
    try:
        trend_engine = ConsensusTrendEngine(db_connection_pool=connection_pool)
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT DISTINCT symbol FROM user_tickers')
                user_symbols = [row[0] for row in cur.fetchall()]
                
                default_symbols = ["SPY", "QQQ", "IWM", "DIA", "AAPL", "NVDA", "MSFT", "TSLA", "GOOGL", "AMZN", "META", "NFLX"]
                trend_symbols = list(set(user_symbols + default_symbols))
        
        logger.info(f"Analyzing trends for {len(trend_symbols)} symbols")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for symbol in trend_symbols:
                    try:
                        result = trend_engine.get_trend_analysis(symbol)
                        
                        if "error" in result:
                            if "Insufficient data" in result['error']:
                                if backfill_user_ticker_on_demand(symbol):
                                    result = trend_engine.get_trend_analysis(symbol)
                                    if "error" in result:
                                        continue
                                else:
                                    continue
                            else:
                                continue
                        
                        signals_json = json.dumps(result.get('signals', []))
                        
                        cur.execute('''
                            INSERT INTO trend_analysis (
                                symbol, trend_score, trend_rating, risk_level,
                                ma_alignment, volume_strength, momentum, 
                                relative_strength, stage_analysis,
                                short_term_trend, medium_term_trend, long_term_trend,
                                alignment_status, signals, timestamp
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                            ON CONFLICT (symbol) DO UPDATE SET
                                trend_score = EXCLUDED.trend_score,
                                trend_rating = EXCLUDED.trend_rating,
                                risk_level = EXCLUDED.risk_level,
                                ma_alignment = EXCLUDED.ma_alignment,
                                volume_strength = EXCLUDED.volume_strength,
                                momentum = EXCLUDED.momentum,
                                relative_strength = EXCLUDED.relative_strength,
                                stage_analysis = EXCLUDED.stage_analysis,
                                short_term_trend = EXCLUDED.short_term_trend,
                                medium_term_trend = EXCLUDED.medium_term_trend,
                                long_term_trend = EXCLUDED.long_term_trend,
                                alignment_status = EXCLUDED.alignment_status,
                                signals = EXCLUDED.signals,
                                timestamp = NOW()
                        ''', (
                            symbol,
                            float(result['trend_score']),                        # Convert to Python float
                            result['trend_rating'],
                            result['risk_level'],
                            float(result['components']['ma_alignment']),         # Convert to Python float
                            float(result['components']['volume_strength']),      # Convert to Python float
                            float(result['components']['momentum']),             # Convert to Python float
                            float(result['components']['relative_strength']),    # Convert to Python float
                            float(result['components']['stage_analysis']),       # Convert to Python float
                            result['timeframe_alignment']['short_term'],
                            result['timeframe_alignment']['medium_term'],
                            result['timeframe_alignment']['long_term'],
                            result['timeframe_alignment']['alignment'],
                            signals_json
                        ))
                        
                        logger.info(f"✅ {symbol} trend score: {result['trend_score']}/100")
                        
                    except Exception as e:
                        logger.error(f"Error analyzing {symbol}: {e}")
                        continue
                
                conn.commit()
                logger.info("✅ Trend analysis data stored successfully")
        
    except Exception as e:
        logger.error(f"Error in trend analysis: {e}")

def populate_sp500_constituents():
    """Fetch and populate S&P 500 constituents from Wikipedia"""
    try:
        import pandas as pd
        
        logger.info("Fetching S&P 500 constituents from Wikipedia...")
        
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)
        sp500_table = tables[0]
        
        logger.info(f"Found {len(sp500_table)} S&P 500 constituents")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM index_constituents WHERE index_name = 'SP500'")
                
                for _, row in sp500_table.iterrows():
                    try:
                        symbol = row['Symbol'].replace('.', '-')
                        company_name = row['Security']
                        sector = row['GICS Sector']
                        industry = row.get('GICS Sub-Industry', 'Unknown')
                        
                        cur.execute('''
                            INSERT INTO index_constituents 
                            (index_name, symbol, company_name, sector, industry, is_active)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (index_name, symbol) DO UPDATE SET
                                company_name = EXCLUDED.company_name,
                                sector = EXCLUDED.sector,
                                industry = EXCLUDED.industry,
                                is_active = EXCLUDED.is_active
                        ''', ('SP500', symbol, company_name, sector, industry, True))
                        
                    except Exception as e:
                        logger.warning(f"Error inserting {row.get('Symbol', 'Unknown')}: {e}")
                        continue
                
                conn.commit()
                
                cur.execute("SELECT COUNT(*) FROM index_constituents WHERE index_name = 'SP500' AND is_active = TRUE")
                count = cur.fetchone()[0]
                logger.info(f"✅ Successfully populated {count} S&P 500 constituents")
                
    except Exception as e:
        logger.error(f"Error populating S&P 500 constituents: {e}")

def calculate_sp500_breadth(target_date=None):
    """Calculate real S&P 500 market breadth metrics"""
    try:
        if target_date is None:
            target_date = datetime.now().date()
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    SELECT symbol FROM index_constituents 
                    WHERE index_name = 'SP500' AND is_active = TRUE
                ''')
                symbols = [row[0] for row in cur.fetchall()]
                
                if not symbols:
                    return None
                
                cur.execute('SELECT MAX(date) FROM daily_prices WHERE date <= %s', (target_date,))
                latest_date = cur.fetchone()[0]
                
                if not latest_date:
                    return None
                
                cur.execute('SELECT MAX(date) FROM daily_prices WHERE date < %s', (latest_date,))
                previous_date = cur.fetchone()[0]
                
                if not previous_date:
                    return None
                
                cur.execute('''
                    SELECT 
                        today.symbol,
                        today.close as today_close,
                        yesterday.close as yesterday_close,
                        today.high,
                        today.low
                    FROM daily_prices today
                    JOIN daily_prices yesterday ON today.symbol = yesterday.symbol
                    JOIN index_constituents ic ON today.symbol = ic.symbol
                    WHERE today.date = %s 
                    AND yesterday.date = %s
                    AND ic.index_name = 'SP500' 
                    AND ic.is_active = TRUE
                    AND today.close IS NOT NULL 
                    AND yesterday.close IS NOT NULL
                ''', (latest_date, previous_date))
                
                results = cur.fetchall()
                
                if not results:
                    return None
                
                advancing = 0
                declining = 0
                unchanged = 0
                
                for row in results:
                    symbol, today_close, yesterday_close, high, low = row
                    
                    if today_close > yesterday_close:
                        advancing += 1
                    elif today_close < yesterday_close:
                        declining += 1
                    else:
                        unchanged += 1
                
                cur.execute('''
                    SELECT 
                        today.symbol,
                        today.high as today_high,
                        today.low as today_low,
                        MAX(hist.high) as week52_high,
                        MIN(hist.low) as week52_low
                    FROM daily_prices today
                    JOIN daily_prices hist ON today.symbol = hist.symbol
                    JOIN index_constituents ic ON today.symbol = ic.symbol
                    WHERE today.date = %s
                    AND hist.date BETWEEN %s AND %s
                    AND ic.index_name = 'SP500'
                    AND ic.is_active = TRUE
                    GROUP BY today.symbol, today.high, today.low
                    HAVING COUNT(hist.date) >= 50
                ''', (latest_date, latest_date - timedelta(days=365), latest_date))
                
                highs_lows_data = cur.fetchall()
                
                new_highs = 0
                new_lows = 0
                
                for row in highs_lows_data:
                    symbol, today_high, today_low, week52_high, week52_low = row
                    
                    if abs(today_high - week52_high) < 0.01:
                        new_highs += 1
                    
                    if abs(today_low - week52_low) < 0.01:
                        new_lows += 1
                
                return {
                    'advancing': advancing,
                    'declining': declining,
                    'unchanged': unchanged,
                    'new_highs': new_highs,
                    'new_lows': new_lows,
                    'total_issues': len(results),
                    'date': latest_date,
                    'comparison_date': previous_date
                }
                
    except Exception as e:
        logger.error(f"Error calculating S&P 500 breadth: {e}")
        return None

def get_real_market_breadth():
    """Get real market breadth data using S&P 500 calculation"""
    try:
        sp500_breadth = calculate_sp500_breadth()
        
        if sp500_breadth:
            return {
                'advancing': sp500_breadth['advancing'],
                'declining': sp500_breadth['declining'], 
                'unchanged': sp500_breadth['unchanged'],
                'new_highs': sp500_breadth['new_highs'],
                'new_lows': sp500_breadth['new_lows'],
                'source': 'sp500_real_data'
            }
        else:
            return {
                'advancing': 0,
                'declining': 0,
                'unchanged': 0,
                'new_highs': 0,
                'new_lows': 0,
                'source': 'calculation_failed'
            }
            
    except Exception as e:
        logger.error(f"Error in get_real_market_breadth: {e}")
        return {
            'advancing': 0,
            'declining': 0,
            'unchanged': 0,
            'new_highs': 0,
            'new_lows': 0,
            'source': 'error_fallback'
        }

def initialize_breadth_system():
   """Initialize the market breadth calculation system"""
   try:
       logger.info("Initializing market breadth system...")
       
       create_breadth_tables()
       
       with get_db_connection() as conn:
           with conn.cursor() as cur:
               cur.execute("SELECT COUNT(*) FROM index_constituents WHERE index_name = 'SP500' AND is_active = TRUE")
               sp500_count = cur.fetchone()[0]
       
       if sp500_count == 0:
           logger.info("No S&P 500 constituents found, fetching from Wikipedia...")
           populate_sp500_constituents()
       else:
           logger.info(f"Found {sp500_count} S&P 500 constituents in database")
       
       logger.info("✅ Market breadth system initialized")
       
   except Exception as e:
       logger.error(f"Error initializing breadth system: {e}")

def fetch_and_store_market_data():
   """Fetch market data and store in database"""
   try:
       logger.info("Fetching and storing market data...")
       
       symbols = {
           'SPY': 'S&P 500',
           'QQQ': 'NASDAQ-100', 
           'IWM': 'Russell 2000',
           'DIA': 'Dow Jones',
           '^VIX': 'VIX'
       }
       
       sector_symbols = {
           'XLK': 'Technology',
           'XLF': 'Financials', 
           'XLV': 'Healthcare',
           'XLE': 'Energy',
           'XLI': 'Industrials',
           'XLC': 'Communication',
           'XLY': 'Consumer Disc.',
           'XLP': 'Consumer Staples',
           'XLB': 'Materials',
           'XLRE': 'Real Estate',
           'XLU': 'Utilities'
       }
       
       current_time = datetime.now()
       
       with get_db_connection() as conn:
           with conn.cursor() as cur:
               # Fetch indices
               for symbol, name in symbols.items():
                   try:
                       session = get_chrome_session()
                       ticker = yf.Ticker(symbol, session=session)
                       hist = ticker.history(period="5d")
                       
                       if not hist.empty:
                           current_price = hist['Close'].iloc[-1]
                           prev_close = hist['Close'].iloc[-2] if len(hist) > 1 else current_price
                           change_pct = ((current_price - prev_close) / prev_close) * 100
                           
                           cur.execute('''
                               INSERT INTO market_indices 
                               (timestamp, symbol, name, price, change_pct, volume, high_52w, low_52w)
                               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                           ''', (
                               current_time, symbol, name,
                               float(current_price),
                               float(change_pct),
                               int(hist['Volume'].iloc[-1]) if 'Volume' in hist.columns else 0,
                               float(hist['High'].max()),
                               float(hist['Low'].min())
                           ))
                   except Exception as e:
                       logger.error(f"Failed to fetch {symbol}: {e}")
                       continue
               
               # Fetch sectors
               for symbol, name in sector_symbols.items():
                   try:
                       session = get_chrome_session()
                       ticker = yf.Ticker(symbol, session=session)
                       hist = ticker.history(period="5d")
                       
                       if not hist.empty:
                           current_price = hist['Close'].iloc[-1]
                           prev_close = hist['Close'].iloc[-2] if len(hist) > 1 else current_price
                           change_pct = ((current_price - prev_close) / prev_close) * 100
                           
                           cur.execute('''
                               INSERT INTO sectors (timestamp, symbol, name, price, change_pct)
                               VALUES (%s, %s, %s, %s, %s)
                           ''', (
                               current_time, symbol, name,
                               float(current_price),
                               float(change_pct)
                           ))
                   except Exception as e:
                       logger.error(f"Failed to fetch sector {symbol}: {e}")
                       continue
               
               # Market breadth
               breadth_data = get_real_market_breadth()
               advancing = breadth_data['advancing']
               declining = breadth_data['declining']
               unchanged = breadth_data['unchanged']
               new_highs = breadth_data['new_highs']
               new_lows = breadth_data['new_lows']
               
               cur.execute('''
                   INSERT INTO market_breadth 
                   (timestamp, advancing, declining, unchanged, advance_decline_ratio, new_highs, new_lows, new_hl_ratio)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ''', (
                   current_time, advancing, declining, unchanged,
                   float(advancing / declining) if declining > 0 else 0.0,
                   new_highs, new_lows,
                   float(new_highs / new_lows) if new_lows > 0 else 0.0
               ))
               
               # Market summary
               cur.execute('SELECT COUNT(*) as positive FROM sectors WHERE timestamp = %s AND change_pct > 0', (current_time,))
               positive_sectors = cur.fetchone()[0]
               
               cur.execute('SELECT COUNT(*) as total FROM sectors WHERE timestamp = %s', (current_time,))
               total_sectors = cur.fetchone()[0]
               
               advance_decline_ratio = advancing / declining if declining > 0 else 0
               overall_trend = "BULLISH" if advance_decline_ratio > 1.5 else "BEARISH" if advance_decline_ratio < 0.8 else "MIXED"
               participation = "BROAD" if positive_sectors / total_sectors > 0.6 else "NARROW"
               momentum = "STRONG" if new_highs / new_lows > 3 else "WEAK" if new_highs / new_lows < 1.5 else "MIXED"
               
               market_score = (
                   (advance_decline_ratio / 3.0 * 40) +
                   (positive_sectors / total_sectors * 40) +
                   (min(new_highs / new_lows / 5.0, 1.0) * 20)
               )
               market_score = min(100, max(0, market_score))
               
               cur.execute('''
                   INSERT INTO market_summary 
                   (timestamp, overall_trend, participation, momentum, positive_sectors, total_sectors, market_score)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
               ''', (
                   current_time, overall_trend, participation, momentum,
                   positive_sectors, total_sectors, float(market_score)
               ))
               
               conn.commit()
               logger.info("Market data stored successfully")
               
               try:
                   fetch_and_store_trend_data()
               except Exception as e:
                   logger.error(f"Error in trend analysis: {e}")
       
   except Exception as e:
       logger.error(f"Error fetching and storing market data: {e}")

def get_latest_market_data():
   """Get latest market data from database"""
   try:
       with get_db_connection() as conn:
           with conn.cursor(cursor_factory=RealDictCursor) as cur:
               cur.execute('''
                   SELECT DISTINCT ON (symbol) symbol, name, price, change_pct, volume, high_52w, low_52w
                   FROM market_indices 
                   ORDER BY symbol, timestamp DESC
               ''')
               indices = {row['symbol']: dict(row) for row in cur.fetchall()}
               
               cur.execute('SELECT * FROM market_breadth ORDER BY timestamp DESC LIMIT 1')
               breadth = dict(cur.fetchone() or {})
               
               cur.execute('''
                   SELECT DISTINCT ON (symbol) symbol, name, price, change_pct
                   FROM sectors 
                   ORDER BY symbol, timestamp DESC
               ''')
               sectors = {row['symbol']: dict(row) for row in cur.fetchall()}
               
               cur.execute('SELECT * FROM market_summary ORDER BY timestamp DESC LIMIT 1')
               summary = dict(cur.fetchone() or {})
               
               return {
                   'indices': indices,
                   'breadth': breadth,
                   'sectors': sectors,
                   'summary': summary,
                   'last_updated': datetime.now()
               }
   except Exception as e:
       logger.error(f"Error getting latest market data: {e}")
       return {}

# Testing endpoint
@app.route('/api/test-market-hours')
def test_market_hours():
   """Test market hours endpoint"""
   try:
       et = pytz.timezone('US/Eastern')
       now_et = datetime.now(et)
       
       is_open, next_open, status = is_market_open()
       
       return jsonify({
           'market_status': status,
           'is_open': is_open,
           'next_open': next_open.isoformat() if next_open else None,
           'debug_info': {
               'current_et_time': now_et.isoformat(),
               'weekday': now_et.weekday(),
               'hour': now_et.hour,
               'minute': now_et.minute,
               'is_weekday': now_et.weekday() < 5
           }
       })
   except Exception as e:
       logger.error(f"Error in test endpoint: {e}")
       return jsonify({'error': str(e)}), 500

# Authentication Routes
@app.route('/login', methods=['GET', 'POST'])
def login():
   if request.method == 'POST':
       email = request.form['email'].lower().strip()
       password = request.form['password']
       
       try:
           with get_db_connection() as conn:
               with conn.cursor(cursor_factory=RealDictCursor) as cur:
                   cur.execute('SELECT * FROM users WHERE email = %s AND is_active = TRUE', (email,))
                   user = cur.fetchone()
                   
                   if user and check_password_hash(user['password_hash'], password):
                       cur.execute('UPDATE users SET last_login = NOW() WHERE id = %s', (user['id'],))
                       conn.commit()
                       
                       user_obj = User(user['id'], user['email'], user['name'], user['created_at'])
                       login_user(user_obj)
                       
                       flash('Login successful!', 'success')
                       return redirect(url_for('dashboard'))
                   else:
                       flash('Invalid email or password', 'error')
       except Exception as e:
           logger.error(f"Login error: {e}")
           flash('Login failed. Please try again.', 'error')
   
   return render_template('login.html')

@app.route('/signup', methods=['GET', 'POST'])
def signup():
   if request.method == 'POST':
       name = request.form['name'].strip()
       email = request.form['email'].lower().strip()
       password = request.form['password']
       confirm_password = request.form['confirm_password']
       
       if not name or not email or not password:
           flash('All fields are required', 'error')
           return render_template('signup.html')
       
       if password != confirm_password:
           flash('Passwords do not match', 'error')
           return render_template('signup.html')
       
       if len(password) < 6:
           flash('Password must be at least 6 characters', 'error')
           return render_template('signup.html')
       
       try:
           with get_db_connection() as conn:
               with conn.cursor() as cur:
                   cur.execute('SELECT id FROM users WHERE email = %s', (email,))
                   if cur.fetchone():
                       flash('Email already registered', 'error')
                       return render_template('signup.html')
                   
                   password_hash = generate_password_hash(password)
                   cur.execute('''
                       INSERT INTO users (email, name, password_hash)
                       VALUES (%s, %s, %s) RETURNING id
                   ''', (email, name, password_hash))
                   
                   user_id = cur.fetchone()[0]
                   
                   default_tickers = [
                       ('SPY', 'index'), ('QQQ', 'index'), ('IWM', 'index'),
                       ('AAPL', 'equity'), ('NVDA', 'equity'), ('MSFT', 'equity')
                   ]
                   
                   for symbol, ticker_type in default_tickers:
                       cur.execute('''
                           INSERT INTO user_tickers (user_id, symbol, ticker_type)
                           VALUES (%s, %s, %s)
                       ''', (user_id, symbol, ticker_type))
                   
                   conn.commit()
                   
                   flash('Account created successfully! Please log in.', 'success')
                   return redirect(url_for('login'))
                   
       except Exception as e:
           logger.error(f"Signup error: {e}")
           flash('Signup failed. Please try again.', 'error')
   
   return render_template('signup.html')

@app.route('/logout')
@login_required
def logout():
   logout_user()
   flash('You have been logged out', 'info')
   return redirect(url_for('login'))

# Main Routes
@app.route('/')
@login_required
def dashboard():
   data = get_latest_market_data()
   return render_template('dashboard.html', data=data)

@app.route('/api/data')
@login_required
def api_data():
   return jsonify(get_latest_market_data())

@app.route('/api/refresh')
@login_required
def refresh_data():
   fetch_and_store_market_data()
   return jsonify({'status': 'success', 'last_updated': datetime.now().isoformat()})

@app.route('/api/trend-data')
@login_required
def get_trend_data():
   """API endpoint to get trend analysis data for current user"""
   try:
       with get_db_connection() as conn:
           with conn.cursor(cursor_factory=RealDictCursor) as cur:
               cur.execute('''
                   SELECT DISTINCT symbol FROM user_tickers 
                   WHERE user_id = %s
               ''', (current_user.id,))
               user_symbols = [row['symbol'] for row in cur.fetchall()]
               
               default_symbols = ['SPY', 'QQQ', 'IWM', 'AAPL', 'NVDA', 'MSFT']
               all_symbols = list(set(user_symbols + default_symbols))
               
               if all_symbols:
                   symbol_placeholders = ','.join(['%s'] * len(all_symbols))
                   cur.execute(f'''
                       SELECT symbol, trend_score, trend_rating, risk_level,
                              ma_alignment, volume_strength, momentum, 
                              relative_strength, stage_analysis,
                              short_term_trend, medium_term_trend, long_term_trend,
                              alignment_status, signals, timestamp
                       FROM trend_analysis 
                       WHERE symbol IN ({symbol_placeholders})
                       ORDER BY trend_score DESC
                   ''', all_symbols)
               else:
                   cur.execute('''
                       SELECT symbol, trend_score, trend_rating, risk_level,
                              ma_alignment, volume_strength, momentum, 
                              relative_strength, stage_analysis,
                              short_term_trend, medium_term_trend, long_term_trend,
                              alignment_status, signals, timestamp
                       FROM trend_analysis 
                       ORDER BY trend_score DESC
                       LIMIT 10
                   ''')
               
               trend_data = []
               for row in cur.fetchall():
                   trend_data.append({
                       'symbol': row['symbol'],
                       'trend_score': float(row['trend_score']) if row['trend_score'] else 0,
                       'trend_rating': row['trend_rating'],
                       'risk_level': row['risk_level'],
                       'components': {
                           'ma_alignment': float(row['ma_alignment']) if row['ma_alignment'] else 0,
                           'volume_strength': float(row['volume_strength']) if row['volume_strength'] else 0,
                           'momentum': float(row['momentum']) if row['momentum'] else 0,
                           'relative_strength': float(row['relative_strength']) if row['relative_strength'] else 0,
                           'stage_analysis': float(row['stage_analysis']) if row['stage_analysis'] else 0
                       },
                       'timeframe_alignment': {
                           'short_term': row['short_term_trend'],
                           'medium_term': row['medium_term_trend'],
                           'long_term': row['long_term_trend'],
                           'alignment': row['alignment_status']
                       },
                       'signals': row['signals'],
                       'timestamp': row['timestamp'].isoformat() if row['timestamp'] else None
                   })
               
               return jsonify(trend_data)
       
   except Exception as e:
       logger.error(f"Error fetching trend data: {e}")
       return jsonify({'error': str(e)}), 500

@app.route('/api/tickers', methods=['GET'])
@login_required
def get_user_tickers():
   """Get current user's saved ticker preferences"""
   try:
       with get_db_connection() as conn:
           with conn.cursor(cursor_factory=RealDictCursor) as cur:
               cur.execute('''
                   SELECT ticker_type, symbol, added_date
                   FROM user_tickers 
                   WHERE user_id = %s
                   ORDER BY added_date ASC
               ''', (current_user.id,))
               
               tickers = cur.fetchall()
               
               result = {
                   'indices': [],
                   'equities': []
               }
               
               for ticker in tickers:
                   if ticker['ticker_type'] == 'index':
                       result['indices'].append(ticker['symbol'])
                   else:
                       result['equities'].append(ticker['symbol'])
               
               if not result['indices']:
                   result['indices'] = ['SPY', 'QQQ', 'IWM']
               if not result['equities']:
                   result['equities'] = ['AAPL', 'NVDA', 'MSFT', 'TSLA', 'GOOGL']
               
               return jsonify(result)
               
   except Exception as e:
       logger.error(f"Error getting user tickers: {e}")
       return jsonify({
           'indices': ['SPY', 'QQQ', 'IWM'],
           'equities': ['AAPL', 'NVDA', 'MSFT', 'TSLA', 'GOOGL']
       })

@app.route('/api/tickers', methods=['POST'])
@login_required
def add_ticker():
   """Add a new ticker for current user with validation and auto-backfill"""
   try:
       data = request.get_json()
       symbol = data.get('symbol', '').upper().strip()
       ticker_type = data.get('type', 'equity')
       
       if not symbol:
           return jsonify({'success': False, 'error': 'Symbol is required'}), 400
       
       is_valid, validation_info = validate_ticker_symbol(symbol)
       if not is_valid:
           return jsonify({'success': False, 'error': validation_info['error']}), 400
       
       with get_db_connection() as conn:
           with conn.cursor() as cur:
               cur.execute('''
                   INSERT INTO user_tickers (user_id, symbol, ticker_type)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (user_id, symbol) DO NOTHING
               ''', (current_user.id, symbol, ticker_type))
               
               if cur.rowcount == 0:
                   return jsonify({'success': False, 'error': 'Ticker already exists'}), 400
               
               conn.commit()
       
       logger.info(f"Added ticker {symbol} for user {current_user.id}")
       
       with get_db_connection() as conn:
           with conn.cursor() as cur:
               cur.execute('''
                   SELECT COUNT(*) FROM daily_prices 
                   WHERE symbol = %s AND date >= CURRENT_DATE - INTERVAL '365 days'
               ''', (symbol,))
               existing_count = cur.fetchone()[0]
               
               needs_backfill = existing_count < 250
       
       if needs_backfill:
           logger.info(f"🚀 Starting immediate backfill for {symbol}...")
           try:
               backfill_success = backfill_user_ticker_on_demand(symbol)
               
               if backfill_success:
                   with get_db_connection() as conn:
                       with conn.cursor() as cur:
                           cur.execute('SELECT COUNT(*) FROM daily_prices WHERE symbol = %s', (symbol,))
                           total_records = cur.fetchone()[0]
                   
                   return jsonify({
                       'success': True, 
                       'symbol': symbol,
                       'backfilled': True,
                       'records': total_records,
                       'message': f'{symbol} added with {total_records} days of historical data'
                   })
               else:
                   return jsonify({
                       'success': True, 
                       'symbol': symbol,
                       'backfilled': False,
                       'message': f'{symbol} added (data will be available on next refresh)'
                   })
                   
           except Exception as e:
               logger.error(f"Error during immediate backfill for {symbol}: {e}")
               return jsonify({
                   'success': True, 
                   'symbol': symbol,
                   'backfilled': False,
                   'message': f'{symbol} added (data will be available on next refresh)'
               })
       else:
           return jsonify({
               'success': True, 
               'symbol': symbol,
               'backfilled': False,
               'existing_records': existing_count,
               'message': f'{symbol} added (already has sufficient historical data)'
           })
       
   except Exception as e:
       logger.error(f"Error adding ticker: {e}")
       return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/tickers/<symbol>', methods=['DELETE'])
@login_required
def remove_ticker(symbol):
   """Remove a ticker from current user's preferences"""
   try:
       symbol = symbol.upper().strip()
       
       with get_db_connection() as conn:
           with conn.cursor() as cur:
               cur.execute('''
                   DELETE FROM user_tickers 
                   WHERE user_id = %s AND symbol = %s
               ''', (current_user.id, symbol))
               
               conn.commit()
               
               if cur.rowcount > 0:
                   logger.info(f"Removed ticker {symbol} for user {current_user.id}")
                   return jsonify({'success': True, 'message': f'{symbol} removed successfully'})
               else:
                   return jsonify({'error': f'{symbol} not found in your list'}), 404
               
   except Exception as e:
       logger.error(f"Error removing ticker {symbol}: {e}")
       return jsonify({'error': 'Server error'}), 500

@app.route('/api/validate-ticker/<symbol>')
@login_required
def validate_ticker_endpoint(symbol):
   """API endpoint to validate a ticker symbol"""
   try:
       is_valid, info = validate_ticker_symbol(symbol)
       return jsonify({
           'valid': is_valid,
           'info': info
       })
   except Exception as e:
       logger.error(f"Error validating ticker {symbol}: {e}")
       return jsonify({'valid': False, 'info': {'error': str(e)}}), 500

if __name__ == '__main__':
   # Initialize database
   if init_db_pool():
       create_tables()
       create_users_table()
       create_user_tickers_table()
       create_trend_table()
       initialize_breadth_system()
       
       # Initial data fetch
       fetch_and_store_market_data()
       
       # Schedule data updates
       scheduler = BackgroundScheduler()
       scheduler.add_job(
           func=fetch_and_store_market_data,
           trigger="interval",
           minutes=APP_CONFIG['refresh_interval'],
           id='market_data_job'
       )
       scheduler.start()
       
       logger.info(f"Scheduler started - data refresh every {APP_CONFIG['refresh_interval']} minutes")
       
       try:
           app.run(host='0.0.0.0', port=5150, debug=(os.environ.get('FLASK_ENV') == 'development'))
       except (KeyboardInterrupt, SystemExit):
           scheduler.shutdown()
   else:
       logger.error("Failed to initialize database. Exiting.")
       exit(1)