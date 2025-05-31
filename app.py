import requests  # Keep for general use
from curl_cffi import requests as curl_requests  # Specific for Yahoo Finance
from flask import Flask, render_template, jsonify, request
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

def create_user_tickers_table():
    """Create user ticker preferences table"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS user_tickers (
                        id SERIAL PRIMARY KEY,
                        user_session VARCHAR(100) NOT NULL,
                        ticker_type VARCHAR(10) NOT NULL, -- 'index' or 'equity'
                        symbol VARCHAR(10) NOT NULL,
                        added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_session, symbol)
                    );
                    CREATE INDEX IF NOT EXISTS idx_user_tickers_session ON user_tickers(user_session);
                ''')
                conn.commit()
                logger.info("Table user_tickers ready")
    except Exception as e:
        logger.error(f"Error creating user_tickers table: {e}")

def create_user_preferences_table():
    """Create user preferences table"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE IF NOT EXISTS user_preferences (
                        id SERIAL PRIMARY KEY,
                        user_session VARCHAR(100) NOT NULL,
                        preference_key VARCHAR(50) NOT NULL,
                        preference_value TEXT,
                        updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(user_session, preference_key)
                    );
                    CREATE INDEX IF NOT EXISTS idx_user_prefs_session ON user_preferences(user_session);
                ''')
                conn.commit()
                logger.info("Table user_preferences ready")
    except Exception as e:
        logger.error(f"Error creating user_preferences table: {e}")

def create_breadth_tables():
    """Create tables for market breadth calculation"""
    tables = {
        'index_constituents': '''
            CREATE TABLE IF NOT EXISTS index_constituents (
                id SERIAL PRIMARY KEY,
                index_name VARCHAR(20) NOT NULL,  -- 'SP500', 'NASDAQ100', 'RUSSELL2000', 'DJIA'
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
                is_final BOOLEAN DEFAULT FALSE,  -- True when market closes for the day
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
                index_name VARCHAR(20) NOT NULL,  -- 'SP500', 'NASDAQ100', etc.
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
                combined_advancing INTEGER DEFAULT 0,    -- All indices combined
                combined_declining INTEGER DEFAULT 0,
                combined_unchanged INTEGER DEFAULT 0,
                combined_new_highs INTEGER DEFAULT 0,
                combined_new_lows INTEGER DEFAULT 0,
                combined_ad_ratio DECIMAL(8,4),
                combined_hl_ratio DECIMAL(8,4),
                market_breadth_score DECIMAL(5,2),  -- 0-100 breadth health score
                breadth_rating VARCHAR(20),  -- 'STRONG', 'WEAK', etc.
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

def get_user_session_id(request):
    """Get or create user session ID"""
    # For now, use a simple approach - could be enhanced with proper user auth
    session_id = request.headers.get('X-Session-ID')
    if not session_id:
        # Generate a simple session ID based on IP + User Agent
        import hashlib
        user_info = f"{request.remote_addr}_{request.headers.get('User-Agent', '')}"
        session_id = hashlib.md5(user_info.encode()).hexdigest()[:20]
    return session_id

def get_chrome_session():
    return curl_requests.Session(impersonate="chrome")

def fetch_and_store_trend_data():
    """Fetch trend analysis for user-configured symbols and store in database"""
    try:
        # Initialize trend engine with database connection pool
        trend_engine = ConsensusTrendEngine(db_connection_pool=connection_pool)
        
        # Get all unique symbols from user preferences
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    SELECT DISTINCT symbol FROM user_tickers
                    UNION
                    SELECT 'SPY' AS symbol
                    UNION  
                    SELECT 'QQQ' AS symbol
                    UNION
                    SELECT 'IWM' AS symbol
                    UNION
                    SELECT 'AAPL' AS symbol
                    UNION
                    SELECT 'NVDA' AS symbol
                ''')
                
                trend_symbols = [row[0] for row in cur.fetchall()]
                
                # Ensure we have some symbols to analyze
                if not trend_symbols:
                    trend_symbols = ["SPY", "QQQ", "IWM", "AAPL", "NVDA", "MSFT", "TSLA", "GOOGL"]
        
        logger.info(f"Analyzing trends for {len(trend_symbols)} symbols: {trend_symbols}")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for symbol in trend_symbols:
                    try:
                        logger.info(f"Analyzing trend for {symbol}...")
                        
                        # Get trend analysis
                        result = trend_engine.get_trend_analysis(symbol)
                        
                        if "error" in result:
                            logger.warning(f"Trend analysis failed for {symbol}: {result['error']}")
                            continue
                        
                        # Prepare signals as JSON string
                        signals_json = str(result.get('signals', []))
                        
                        # Store in database (INSERT ... ON CONFLICT UPDATE)
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
                            result['trend_score'],
                            result['trend_rating'],
                            result['risk_level'],
                            result['components']['ma_alignment'],
                            result['components']['volume_strength'],
                            result['components']['momentum'],
                            result['components']['relative_strength'],
                            result['components']['stage_analysis'],
                            result['timeframe_alignment']['short_term'],
                            result['timeframe_alignment']['medium_term'],
                            result['timeframe_alignment']['long_term'],
                            result['timeframe_alignment']['alignment'],
                            signals_json
                        ))
                        
                        logger.info(f"✅ {symbol} trend score: {result['trend_score']}/100 ({result['trend_rating']})")
                        
                    except Exception as e:
                        logger.error(f"Error analyzing {symbol}: {e}")
                        continue
                
                conn.commit()
                logger.info("✅ Trend analysis data stored successfully")
        
    except Exception as e:
        logger.error(f"Error in trend analysis: {e}")

def fetch_and_store_market_data():
    """Fetch market data and store in database"""
    try:
        logger.info("Fetching and storing market data...")
        
        # Define symbols to fetch
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
                # Fetch and store index data
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
                                round(current_price, 4),
                                round(change_pct, 4),
                                int(hist['Volume'].iloc[-1]) if 'Volume' in hist.columns else 0,
                                round(hist['High'].max(), 4),
                                round(hist['Low'].min(), 4)
                            ))
                    except Exception as e:
                        logger.error(f"Failed to fetch {symbol}: {e}")
                        continue
                
                # Fetch and store sector data
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
                                round(current_price, 4),
                                round(change_pct, 4)
                            ))
                    except Exception as e:
                        logger.error(f"Failed to fetch sector {symbol}: {e}")
                        continue
                
                # Market breadth data - REAL S&P 500 calculation
                logger.info("Calculating real market breadth...")
                breadth_data = get_real_market_breadth()

                advancing = breadth_data['advancing']
                declining = breadth_data['declining']
                unchanged = breadth_data['unchanged']
                new_highs = breadth_data['new_highs']
                new_lows = breadth_data['new_lows']

                logger.info(f"Real breadth: A/D {advancing}/{declining}, H/L {new_highs}/{new_lows} (source: {breadth_data['source']})")
                
                cur.execute('''
                    INSERT INTO market_breadth 
                    (timestamp, advancing, declining, unchanged, advance_decline_ratio, new_highs, new_lows, new_hl_ratio)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    current_time, advancing, declining, unchanged,
                    round(advancing / declining, 4),
                    new_highs, new_lows,
                    round(new_highs / new_lows, 4) if new_lows > 0 else 0
                ))
                
                # Calculate and store market summary
                cur.execute('''
                    SELECT COUNT(*) as positive FROM sectors 
                    WHERE timestamp = %s AND change_pct > 0
                ''', (current_time,))
                positive_sectors = cur.fetchone()[0]
                
                cur.execute('''
                    SELECT COUNT(*) as total FROM sectors 
                    WHERE timestamp = %s
                ''', (current_time,))
                total_sectors = cur.fetchone()[0]
                
                advance_decline_ratio = advancing / declining if declining > 0 else 0
                overall_trend = "BULLISH" if advance_decline_ratio > 1.5 else "BEARISH" if advance_decline_ratio < 0.8 else "MIXED"
                participation = "BROAD" if positive_sectors / total_sectors > 0.6 else "NARROW"
                momentum = "STRONG" if new_highs / new_lows > 3 else "WEAK" if new_highs / new_lows < 1.5 else "MIXED"
                
                # Calculate market score (0-100)
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
                    positive_sectors, total_sectors, round(market_score, 2)
                ))
                
                conn.commit()
                logger.info("Market data stored successfully")
                
                # Add trend analysis
                try:
                    logger.info("Starting trend analysis...")
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
                # Get latest indices
                cur.execute('''
                    SELECT DISTINCT ON (symbol) symbol, name, price, change_pct, volume, high_52w, low_52w
                    FROM market_indices 
                    ORDER BY symbol, timestamp DESC
                ''')
                indices = {row['symbol']: dict(row) for row in cur.fetchall()}
                
                # Get latest breadth
                cur.execute('''
                    SELECT * FROM market_breadth 
                    ORDER BY timestamp DESC LIMIT 1
                ''')
                breadth = dict(cur.fetchone() or {})
                
                # Get latest sectors
                cur.execute('''
                    SELECT DISTINCT ON (symbol) symbol, name, price, change_pct
                    FROM sectors 
                    ORDER BY symbol, timestamp DESC
                ''')
                sectors = {row['symbol']: dict(row) for row in cur.fetchall()}
                
                # Get latest summary
                cur.execute('''
                    SELECT * FROM market_summary 
                    ORDER BY timestamp DESC LIMIT 1
                ''')
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

def populate_sp500_constituents():
    """Fetch and populate S&P 500 constituents from Wikipedia"""
    try:
        import pandas as pd
        
        logger.info("Fetching S&P 500 constituents from Wikipedia...")
        
        # Fetch from Wikipedia
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)
        sp500_table = tables[0]  # First table is current constituents
        
        logger.info(f"Found {len(sp500_table)} S&P 500 constituents")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Clear existing S&P 500 data
                cur.execute("DELETE FROM index_constituents WHERE index_name = 'SP500'")
                
                # Insert new constituents
                for _, row in sp500_table.iterrows():
                    try:
                        symbol = row['Symbol'].replace('.', '-')  # Handle BRK.B -> BRK-B
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
                
                # Verify count
                cur.execute("SELECT COUNT(*) FROM index_constituents WHERE index_name = 'SP500' AND is_active = TRUE")
                count = cur.fetchone()[0]
                logger.info(f"✅ Successfully populated {count} S&P 500 constituents")
                
    except Exception as e:
        logger.error(f"Error populating S&P 500 constituents: {e}")
        logger.info("💡 You can manually load constituents from your ToS CSV if needed")

def backfill_daily_prices(days_back=None):
    """
    Backfill daily price data for S&P 500 constituents
    Now uses 2 years of data to ensure we get proper trading day coverage
    """
    try:
        # Use environment config
        if days_back is None:
            days_back = APP_CONFIG['backfill_days']
            
        logger.info(f"Starting backfill of {days_back} calendar days (~{int(days_back * 0.69)} trading days) of daily price data...")
        
        # Get S&P 500 constituents
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    SELECT symbol FROM index_constituents 
                    WHERE index_name = 'SP500' AND is_active = TRUE
                    ORDER BY symbol
                ''')
                symbols = [row[0] for row in cur.fetchall()]
        
        if not symbols:
            logger.error("No S&P 500 constituents found. Run populate_sp500_constituents() first.")
            return
        
        logger.info(f"Backfilling data for {len(symbols)} symbols...")
        
        # Calculate date range - use calendar days for Yahoo Finance
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"Date range: {start_date} to {end_date}")
        
        # Process in batches to avoid overwhelming the API
        batch_size = 50
        total_records_added = 0
        total_duplicates_skipped = 0
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(symbols)-1)//batch_size + 1}: {len(batch)} symbols")
            
            for symbol in batch:
                try:
                    # Fetch historical data with calendar days
                    session = get_chrome_session()
                    ticker = yf.Ticker(symbol, session=session)
                    hist = ticker.history(start=start_date, end=end_date)
                    
                    if hist.empty:
                        logger.warning(f"No data for {symbol}")
                        continue
                    
                    # Store in database and track duplicates
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            records_added = 0
                            duplicates_skipped = 0
                            
                            for date, row in hist.iterrows():
                                try:
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
                                    
                                    # Check if record was actually inserted
                                    if cur.rowcount > 0:
                                        records_added += 1
                                    else:
                                        duplicates_skipped += 1
                                        
                                except Exception as e:
                                    logger.warning(f"Error storing {symbol} {date}: {e}")
                                    continue
                            
                            conn.commit()
                            total_records_added += records_added
                            total_duplicates_skipped += duplicates_skipped
                            
                            logger.info(f"✅ {symbol}: {records_added} new records, {duplicates_skipped} duplicates skipped ({len(hist)} total days from Yahoo)")
                    
                except Exception as e:
                    logger.error(f"Error processing {symbol}: {e}")
                    continue
        
        logger.info(f"🎉 Backfill completed successfully!")
        logger.info(f"📊 Summary: {total_records_added} new records added, {total_duplicates_skipped} duplicates skipped")
        
    except Exception as e:
        logger.error(f"Error in backfill_daily_prices: {e}")

def initialize_breadth_system():
    """Initialize the market breadth calculation system"""
    try:
        logger.info("Initializing market breadth system...")
        
        # Create tables
        create_breadth_tables()
        
        # Check if S&P 500 constituents exist
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM index_constituents WHERE index_name = 'SP500' AND is_active = TRUE")
                sp500_count = cur.fetchone()[0]
        
        # Populate constituents if needed
        if sp500_count == 0:
            logger.info("No S&P 500 constituents found, fetching from Wikipedia...")
            populate_sp500_constituents()
        else:
            logger.info(f"Found {sp500_count} S&P 500 constituents in database")
        
        # Check if we need to backfill price data
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(DISTINCT symbol) FROM daily_prices")
                price_symbols_count = cur.fetchone()[0]
        
        if price_symbols_count < 400:  # Assuming we should have most S&P 500 stocks
            logger.info("Limited price data found, starting backfill...")
            # You can uncomment this for initial setup, but comment it out for normal operation
            backfill_daily_prices(252)
        else:
            logger.info(f"Found price data for {price_symbols_count} symbols")
        
        logger.info("✅ Market breadth system initialized")
        
    except Exception as e:
        logger.error(f"Error initializing breadth system: {e}")

def calculate_sp500_breadth(target_date=None):
    """
    Calculate real S&P 500 market breadth metrics
    Returns advancing, declining, unchanged, new_highs, new_lows
    """
    try:
        from datetime import datetime, timedelta
        
        if target_date is None:
            target_date = datetime.now().date()
        
        logger.info(f"Calculating S&P 500 breadth for {target_date}")
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get all S&P 500 constituents
                cur.execute('''
                    SELECT symbol FROM index_constituents 
                    WHERE index_name = 'SP500' AND is_active = TRUE
                ''')
                symbols = [row[0] for row in cur.fetchall()]
                
                if not symbols:
                    logger.error("No S&P 500 constituents found")
                    return None
                
                # Find the most recent trading day with data (in case target_date is weekend/holiday)
                cur.execute('''
                    SELECT MAX(date) FROM daily_prices 
                    WHERE date <= %s
                ''', (target_date,))
                latest_date = cur.fetchone()[0]
                
                if not latest_date:
                    logger.error("No price data found")
                    return None
                
                # Find the previous trading day
                cur.execute('''
                    SELECT MAX(date) FROM daily_prices 
                    WHERE date < %s
                ''', (latest_date,))
                previous_date = cur.fetchone()[0]
                
                if not previous_date:
                    logger.error("No previous day data found")
                    return None
                
                logger.info(f"Comparing {latest_date} vs {previous_date}")
                
                # Calculate advancing/declining
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
                    logger.error("No matching price data found for breadth calculation")
                    return None
                
                # Count advancing/declining/unchanged
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
                
                # Calculate new highs/lows (52-week)
                # Get 52-week (252 trading days) high/low for each stock
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
                    HAVING COUNT(hist.date) >= 50  -- Ensure sufficient data
                ''', (latest_date, latest_date - timedelta(days=365), latest_date))
                
                highs_lows_data = cur.fetchall()
                
                new_highs = 0
                new_lows = 0
                
                for row in highs_lows_data:
                    symbol, today_high, today_low, week52_high, week52_low = row
                    
                    # New high if today's high equals the 52-week high
                    if abs(today_high - week52_high) < 0.01:  # Small tolerance for float comparison
                        new_highs += 1
                    
                    # New low if today's low equals the 52-week low
                    if abs(today_low - week52_low) < 0.01:
                        new_lows += 1
                
                breadth_data = {
                    'advancing': advancing,
                    'declining': declining,
                    'unchanged': unchanged,
                    'new_highs': new_highs,
                    'new_lows': new_lows,
                    'total_issues': len(results),
                    'date': latest_date,
                    'comparison_date': previous_date
                }
                
                logger.info(f"S&P 500 Breadth: {advancing} advancing, {declining} declining, {unchanged} unchanged")
                logger.info(f"New H/L: {new_highs} highs, {new_lows} lows (out of {len(highs_lows_data)} stocks)")
                
                return breadth_data
                
    except Exception as e:
        logger.error(f"Error calculating S&P 500 breadth: {e}")
        return None

def get_real_market_breadth():
    """
    Updated version - uses real S&P 500 data instead of random numbers
    """
    try:
        # Calculate real S&P 500 breadth
        sp500_breadth = calculate_sp500_breadth()
        
        if sp500_breadth:
            # Use real S&P 500 data
            return {
                'advancing': sp500_breadth['advancing'],
                'declining': sp500_breadth['declining'], 
                'unchanged': sp500_breadth['unchanged'],
                'new_highs': sp500_breadth['new_highs'],
                'new_lows': sp500_breadth['new_lows'],
                'source': 'sp500_real_data'
            }
        else:
            # Return zeros if calculation fails
            logger.warning("S&P 500 breadth calculation failed, returning zeros")
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
        # Return zeros on error - honest about calculation failure
        return {
            'advancing': 0,
            'declining': 0,
            'unchanged': 0,
            'new_highs': 0,
            'new_lows': 0,
            'source': 'error_fallback'
        }

def is_market_open():
    """
    Check if US stock market is currently open
    Returns: (is_open: bool, next_open: datetime, market_status: str)
    """
    try:
        # US Eastern Time
        et = pytz.timezone('US/Eastern')
        now_et = datetime.now(et)
        
        # Market hours: 9:30 AM - 4:00 PM ET, Monday-Friday
        market_open_time = now_et.replace(hour=9, minute=30, second=0, microsecond=0)
        market_close_time = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
        
        # Check if it's a weekday (0=Monday, 4=Friday, 5=Saturday, 6=Sunday)
        is_weekday = now_et.weekday() < 5
        
        # Check if within market hours
        is_open = (is_weekday and market_open_time <= now_et <= market_close_time)
        
        # Calculate next market open and status
        if is_open:
            next_open = now_et
            status = "MARKET OPEN"
        elif is_weekday and now_et < market_open_time:
            next_open = market_open_time
            status = "PRE-MARKET"
        elif is_weekday and now_et > market_close_time:
            # After hours on a weekday - next open is tomorrow
            tomorrow = now_et + timedelta(days=1)
            next_open = tomorrow.replace(hour=9, minute=30, second=0, microsecond=0)
            status = "AFTER-HOURS"
        else:
            # Weekend - find next Monday
            days_ahead = 7 - now_et.weekday()  # Days until next Monday
            if now_et.weekday() == 6:  # Sunday
                days_ahead = 1
            elif now_et.weekday() == 5:  # Saturday
                days_ahead = 2
            
            next_monday = now_et + timedelta(days=days_ahead)
            next_open = next_monday.replace(hour=9, minute=30, second=0, microsecond=0)
            status = "MARKET CLOSED - WEEKEND"
        
        return is_open, next_open, status
        
    except Exception as e:
        logger.error(f"Error checking market hours: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False, None, "UNKNOWN"

def validate_ticker_symbol(symbol):
    """
    Validate if a ticker symbol exists and is tradeable
    Returns: (is_valid: bool, info: dict)
    """
    try:
        symbol = symbol.upper().strip()
        
        # Basic validation
        if not symbol or len(symbol) < 1 or len(symbol) > 5:
            return False, {'error': 'Invalid ticker format'}
        
        # Check with yfinance
        session = get_chrome_session()
        ticker = yf.Ticker(symbol, session=session)
        
        # Try to get recent data
        hist = ticker.history(period="5d")
        info = ticker.info
        
        if hist.empty:
            return False, {'error': f'No trading data found for {symbol}'}
        
        # Additional validation checks
        if 'longName' in info or 'shortName' in info:
            return True, {
                'symbol': symbol,
                'name': info.get('longName', info.get('shortName', symbol)),
                'sector': info.get('sector', 'Unknown'),
                'last_price': float(hist['Close'].iloc[-1]) if not hist.empty else None
            }
        else:
            # Sometimes yfinance returns data but limited info
            return True, {
                'symbol': symbol,
                'name': symbol,
                'sector': 'Unknown',
                'last_price': float(hist['Close'].iloc[-1]) if not hist.empty else None
            }
            
    except Exception as e:
        return False, {'error': f'Error validating {symbol}: {str(e)}'}

# def get_real_market_breadth():
#     """
#     Get real market breadth data from reliable sources
#     Returns: dict with breadth metrics or None if unavailable
#     """
#     try:
#         # Method 1: Use major indices to estimate breadth
#         symbols = ['SPY', 'QQQ', 'IWM', 'DIA']
#         advancing = 0
#         declining = 0
#         unchanged = 0
        
#         session = get_chrome_session()
        
#         for symbol in symbols:
#             try:
#                 ticker = yf.Ticker(symbol, session=session)
#                 hist = ticker.history(period="2d")
                
#                 if len(hist) >= 2:
#                     current = hist['Close'].iloc[-1]
#                     previous = hist['Close'].iloc[-2]
#                     change = (current - previous) / previous
                    
#                     if change > 0.001:  # >0.1% gain
#                         advancing += 1
#                     elif change < -0.001:  # >0.1% loss
#                         declining += 1
#                     else:
#                         unchanged += 1
                        
#             except Exception as e:
#                 logger.error(f"Error fetching {symbol}: {e}")
#                 continue
        
#         # Scale up the counts (these are just indices, real market has thousands)
#         if advancing + declining + unchanged > 0:
#             total_estimate = 4000  # Approximate number of actively traded stocks
#             scale_factor = total_estimate / (advancing + declining + unchanged)
            
#             return {
#                 'advancing': int(advancing * scale_factor * 0.6),  # Weighted estimate
#                 'declining': int(declining * scale_factor * 0.6),
#                 'unchanged': int(unchanged * scale_factor * 0.6),
#                 'new_highs': int(np.random.randint(50, 300)),  # Still estimated
#                 'new_lows': int(np.random.randint(20, 150)),
#                 'source': 'estimated_from_indices'
#             }
    
#     except Exception as e:
#         logger.error(f"Error getting real market breadth: {e}")
    
#     return None

def get_real_market_breadth():
    """
    Updated version - uses real S&P 500 data instead of random numbers
    """
    try:
        logger.info("🔍 DEBUG: Starting market breadth calculation...")
        
        # Calculate real S&P 500 breadth
        sp500_breadth = calculate_sp500_breadth()
        
        # DEBUG BLOCK:
        if sp500_breadth is None:
            logger.error("🚨 DEBUG: calculate_sp500_breadth() returned None")
        else:
            logger.info(f"🔍 DEBUG: S&P 500 calculation succeeded: {sp500_breadth}")
        
        if sp500_breadth:
            # Use real S&P 500 data
            return {
                'advancing': sp500_breadth['advancing'],
                'declining': sp500_breadth['declining'], 
                'unchanged': sp500_breadth['unchanged'],
                'new_highs': sp500_breadth['new_highs'],
                'new_lows': sp500_breadth['new_lows'],
                'source': 'sp500_real_data'
            }
        else:
            # Return zeros if calculation fails
            logger.warning("S&P 500 breadth calculation failed, returning zeros")
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
        # Return zeros on error - honest about calculation failure
        return {
            'advancing': 0,
            'declining': 0,
            'unchanged': 0,
            'new_highs': 0,
            'new_lows': 0,
            'source': 'error_fallback'
        }

def get_trend_data_from_db(symbol, days=100):
    """
    Get historical price data from our database instead of Yahoo Finance
    Much faster for trend analysis! Returns pandas DataFrame or None
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    SELECT date, open, high, low, close, volume
                    FROM daily_prices 
                    WHERE symbol = %s 
                    AND date >= CURRENT_DATE - INTERVAL '%s days'
                    ORDER BY date ASC
                ''', (symbol, days + 50))  # Buffer for weekends/holidays
                
                results = cur.fetchall()
                
                if len(results) < days * 0.7:  # Need at least 70% of requested days
                    logger.info(f"📊 {symbol}: Insufficient DB data ({len(results)} days), will use Yahoo")
                    return None
                
                # Convert to pandas DataFrame (matching Yahoo Finance format)
                import pandas as pd
                
                df = pd.DataFrame(results, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
                df['Date'] = pd.to_datetime(df['Date'])
                df.set_index('Date', inplace=True)
                
                # Rename columns to match Yahoo Finance format
                df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
                
                logger.info(f"⚡ {symbol}: Retrieved {len(df)} days from database (FAST!)")
                return df
                
    except Exception as e:
        logger.error(f"Error getting DB trend data for {symbol}: {e}")
        return None

def analyze_trend_enhanced(symbol, days=100):
    """
    Enhanced version of analyze_trend that uses database first, Yahoo as fallback
    This should replace your existing analyze_trend function
    """
    try:
        logger.info(f"Analyzing trend for {symbol}...")
        
        # STEP 1: Try to get data from database (FAST!)
        hist = get_trend_data_from_db(symbol, days)
        data_source = "database"
        
        # STEP 2: Fallback to Yahoo Finance if no DB data
        if hist is None:
            logger.info(f"🔄 {symbol}: Fetching from Yahoo Finance (fallback)")
            session = get_chrome_session()
            ticker = yf.Ticker(symbol, session=session)
            hist = ticker.history(period=f"{days}d")
            data_source = "yahoo_finance"
            
            if hist.empty:
                logger.error(f"❌ {symbol}: No data available from any source")
                return None
        
        # STEP 3: Your existing trend analysis logic (unchanged)
        if len(hist) < 50:
            logger.error(f"Insufficient data for {symbol}")
            return None
        
        # Calculate technical indicators (your existing code)
        hist['SMA_20'] = hist['Close'].rolling(window=20).mean()
        hist['SMA_50'] = hist['Close'].rolling(window=50).mean()
        hist['EMA_12'] = hist['Close'].ewm(span=12).mean()
        hist['EMA_26'] = hist['Close'].ewm(span=26).mean()
        
        # MACD
        hist['MACD'] = hist['EMA_12'] - hist['EMA_26']
        hist['MACD_Signal'] = hist['MACD'].ewm(span=9).mean()
        hist['MACD_Histogram'] = hist['MACD'] - hist['MACD_Signal']
        
        # RSI
        delta = hist['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        hist['RSI'] = 100 - (100 / (1 + rs))
        
        # Bollinger Bands
        hist['BB_Middle'] = hist['Close'].rolling(window=20).mean()
        bb_std = hist['Close'].rolling(window=20).std()
        hist['BB_Upper'] = hist['BB_Middle'] + (bb_std * 2)
        hist['BB_Lower'] = hist['BB_Middle'] - (bb_std * 2)
        
        # Volume analysis
        hist['Volume_SMA'] = hist['Volume'].rolling(window=20).mean()
        
        # Get latest values for scoring
        latest = hist.iloc[-1]
        
        # Your existing scoring logic (keep this exactly the same)
        score = 0
        max_score = 100
        
        # Trend Analysis (40 points max)
        if latest['Close'] > latest['SMA_20']:
            score += 10
        if latest['Close'] > latest['SMA_50']:
            score += 15
        if latest['SMA_20'] > latest['SMA_50']:
            score += 15
        
        # MACD Analysis (20 points max)
        if latest['MACD'] > latest['MACD_Signal']:
            score += 10
        if latest['MACD'] > 0:
            score += 10
        
        # RSI Analysis (20 points max)
        rsi = latest['RSI']
        if 30 <= rsi <= 70:
            score += 20
        elif 20 <= rsi < 30 or 70 < rsi <= 80:
            score += 10
        
        # Bollinger Bands (10 points max)
        if latest['BB_Lower'] <= latest['Close'] <= latest['BB_Upper']:
            score += 10
        
        # Volume confirmation (10 points max)
        if latest['Volume'] > latest['Volume_SMA']:
            score += 10
        
        # Determine trend strength
        if score >= 80:
            trend = "STRONG BULLISH"
        elif score >= 65:
            trend = "BULLISH"
        elif score >= 45:
            trend = "NEUTRAL BULLISH"
        elif score >= 35:
            trend = "NEUTRAL"
        elif score >= 20:
            trend = "BEARISH"
        else:
            trend = "STRONG BEARISH"
        
        logger.info(f"✅ {symbol} trend score: {score}/100 ({trend}) - Source: {data_source}")
        
        return {
            'symbol': symbol,
            'score': float(score),
            'trend': trend,
            'current_price': float(latest['Close']),
            'sma_20': float(latest['SMA_20']),
            'sma_50': float(latest['SMA_50']),
            'rsi': float(latest['RSI']),
            'macd': float(latest['MACD']),
            'volume_ratio': float(latest['Volume'] / latest['Volume_SMA']),
            'data_source': data_source  # Track where data came from
        }
        
    except Exception as e:
        logger.error(f"Trend analysis failed for {symbol}: {e}")
        return None

def backfill_user_ticker_on_demand(symbol):
    """
    When trend analysis fails due to missing data, automatically backfill
    """
    try:
        logger.info(f"🔄 Auto-backfilling {symbol} due to missing trend data...")
        
        # Check if we have any data
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT COUNT(*) FROM daily_prices WHERE symbol = %s', (symbol,))
                existing_count = cur.fetchone()[0]
        
        if existing_count > 50:
            logger.info(f"✅ {symbol}: Sufficient data exists ({existing_count} records)")
            return True
        
        # Backfill data
        from datetime import datetime, timedelta
        import yfinance as yf
        
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=300)  # Get more data for trend analysis
        
        session = get_chrome_session()
        ticker = yf.Ticker(symbol, session=session)
        hist = ticker.history(start=start_date, end=end_date)
        
        if hist.empty:
            logger.warning(f"❌ {symbol}: No historical data available")
            return False
        
        # Store in database
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

# testing endpoint
@app.route('/api/test-market-hours')
def test_market_hours():
    """Enhanced test endpoint with debugging"""
    try:
        import pytz
        from datetime import datetime, timedelta
        
        # Test timezone
        et = pytz.timezone('US/Eastern')
        now_et = datetime.now(et)
        
        logger.info(f"Current ET time: {now_et}")
        logger.info(f"Weekday: {now_et.weekday()}")  # 0=Monday, 6=Sunday
        logger.info(f"Hour: {now_et.hour}, Minute: {now_et.minute}")
        
        # Call the function
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
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()}), 500

@app.route('/')
def dashboard():
    data = get_latest_market_data()
    return render_template('dashboard.html', data=data)

@app.route('/api/data')
def api_data():
    return jsonify(get_latest_market_data())

@app.route('/api/refresh')
def refresh_data():
    fetch_and_store_market_data()
    return jsonify({'status': 'success', 'last_updated': datetime.now().isoformat()})

@app.route('/api/trend-data')
def get_trend_data():
    """API endpoint to get trend analysis data"""
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute('''
                    SELECT symbol, trend_score, trend_rating, risk_level,
                           ma_alignment, volume_strength, momentum, 
                           relative_strength, stage_analysis,
                           short_term_trend, medium_term_trend, long_term_trend,
                           alignment_status, signals, timestamp
                    FROM trend_analysis 
                    ORDER BY trend_score DESC
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
def get_user_tickers():
    """Get user's saved ticker preferences"""
    try:
        session_id = get_user_session_id(request)
        
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute('''
                    SELECT ticker_type, symbol, added_date
                    FROM user_tickers 
                    WHERE user_session = %s
                    ORDER BY added_date ASC
                ''', (session_id,))
                
                tickers = cur.fetchall()
                
                # Organize by type
                result = {
                    'indices': [],
                    'equities': []
                }
                
                for ticker in tickers:
                    if ticker['ticker_type'] == 'index':
                        result['indices'].append(ticker['symbol'])
                    else:
                        result['equities'].append(ticker['symbol'])
                
                # Set defaults if empty
                if not result['indices']:
                    result['indices'] = ['SPY', 'QQQ', 'IWM']
                if not result['equities']:
                    result['equities'] = ['AAPL', 'NVDA', 'MSFT', 'TSLA', 'GOOGL']
                
                return jsonify(result)
                
    except Exception as e:
        logger.error(f"Error getting user tickers: {e}")
        # Return defaults on error
        return jsonify({
            'indices': ['SPY', 'QQQ', 'IWM'],
            'equities': ['AAPL', 'NVDA', 'MSFT', 'TSLA', 'GOOGL']
        })

@app.route('/api/tickers', methods=['POST'])
def add_ticker():
    """
    Add a new ticker and immediately backfill its data
    Returns status updates for real-time feedback
    """
    try:
        data = request.get_json()
        symbol = data.get('symbol', '').upper().strip()
        
        if not symbol:
            return jsonify({'success': False, 'error': 'Symbol is required'}), 400
        
        session_id = get_session_id()
        
        # Store ticker immediately
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO user_tickers (session_id, symbol, added_at)
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (session_id, symbol) DO NOTHING
                ''', (session_id, symbol))
                
                if cur.rowcount == 0:
                    return jsonify({'success': False, 'error': 'Ticker already exists'}), 400
                
                conn.commit()
        
        logger.info(f"Added ticker {symbol} for session {session_id}")
        
        # Check if immediate backfill is needed
        needs_backfill = False
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    SELECT COUNT(*) FROM daily_prices 
                    WHERE symbol = %s AND date >= CURRENT_DATE - INTERVAL '365 days'
                ''', (symbol,))
                existing_count = cur.fetchone()[0]
                
                if existing_count < 250:  # Need ~1 year of trading days
                    needs_backfill = True
        
        if needs_backfill:
            # Start immediate backfill in background
            logger.info(f"🚀 Starting immediate backfill for {symbol}...")
            
            try:
                # Use your existing trend engine for consistency
                from trend_engine import TrendEngine
                trend_engine = TrendEngine()
                
                # Perform the backfill
                backfill_success = trend_engine.backfill_symbol_on_demand(symbol)
                
                if backfill_success:
                    # Get the count after backfill
                    with get_db_connection() as conn:
                        with conn.cursor() as cur:
                            cur.execute('''
                                SELECT COUNT(*) FROM daily_prices 
                                WHERE symbol = %s
                            ''', (symbol,))
                            total_records = cur.fetchone()[0]
                    
                    logger.info(f"✅ {symbol}: Immediate backfill completed ({total_records} total records)")
                    
                    return jsonify({
                        'success': True, 
                        'symbol': symbol,
                        'backfilled': True,
                        'records': total_records,
                        'message': f'{symbol} added with {total_records} days of historical data'
                    })
                else:
                    logger.warning(f"⚠️ {symbol}: Backfill failed, but ticker added")
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
def remove_user_ticker(symbol):
    """Remove a ticker from user's preferences"""
    try:
        symbol = symbol.upper().strip()
        session_id = get_user_session_id(request)
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    DELETE FROM user_tickers 
                    WHERE user_session = %s AND symbol = %s
                ''', (session_id, symbol))
                
                conn.commit()
                
                if cur.rowcount > 0:
                    logger.info(f"Removed ticker {symbol} for session {session_id}")
                    return jsonify({'success': True, 'message': f'{symbol} removed successfully'})
                else:
                    return jsonify({'error': f'{symbol} not found in your list'}), 404
                
    except Exception as e:
        logger.error(f"Error removing ticker {symbol}: {e}")
        return jsonify({'error': 'Server error'}), 500

@app.route('/api/user-preferences', methods=['GET'])
def get_user_preferences():
    """Get user's saved preferences"""
    try:
        session_id = get_user_session_id(request)
        
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute('''
                    SELECT preference_key, preference_value
                    FROM user_preferences 
                    WHERE user_session = %s
                ''', (session_id,))
                
                prefs = cur.fetchall()
                
                # Convert to dictionary
                result = {}
                for pref in prefs:
                    result[pref['preference_key']] = pref['preference_value']
                
                return jsonify(result)
                
    except Exception as e:
        logger.error(f"Error getting user preferences: {e}")
        return jsonify({})

@app.route('/api/user-preferences', methods=['POST'])
def save_user_preference():
    """Save a user preference"""
    try:
        data = request.get_json()
        preference_key = data.get('preference_key', '').strip()
        preference_value = data.get('preference_value', '')
        
        if not preference_key:
            return jsonify({'error': 'Invalid preference key'}), 400
        
        session_id = get_user_session_id(request)
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                    INSERT INTO user_preferences (user_session, preference_key, preference_value, updated_date)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (user_session, preference_key) DO UPDATE SET
                        preference_value = EXCLUDED.preference_value,
                        updated_date = NOW()
                ''', (session_id, preference_key, preference_value))
                
                conn.commit()
                
                logger.info(f"Saved preference {preference_key}={preference_value} for session {session_id}")
                return jsonify({'success': True, 'message': 'Preference saved'})
                
    except Exception as e:
        logger.error(f"Error saving user preference: {e}")
        return jsonify({'error': 'Server error'}), 500

if __name__ == '__main__':
    # Initialize database
    if init_db_pool():
        create_tables()
        create_trend_table()
        create_user_tickers_table()
        create_user_preferences_table()
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