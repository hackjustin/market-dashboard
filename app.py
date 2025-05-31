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
    'enable_analytics': os.environ.get('ENABLE_ADVANCED_ANALYTICS', 'true').lower() == 'true'
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
        # Initialize trend engine
        trend_engine = ConsensusTrendEngine()
        
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
                
                # Calculate and store breadth metrics (simulated for demo)
                advancing = np.random.randint(2500, 3500)
                declining = np.random.randint(1000, 2000)
                unchanged = np.random.randint(200, 500)
                new_highs = np.random.randint(150, 400)
                new_lows = np.random.randint(30, 100)
                
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
def add_user_ticker():
    """Add a ticker to user's preferences"""
    try:
        data = request.get_json()
        symbol = data.get('symbol', '').upper().strip()
        ticker_type = data.get('type', 'equity')  # 'index' or 'equity'
        
        if not symbol or len(symbol) > 10:
            return jsonify({'error': 'Invalid ticker symbol'}), 400
        
        session_id = get_user_session_id(request)
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute('''
                        INSERT INTO user_tickers (user_session, ticker_type, symbol)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (user_session, symbol) DO NOTHING
                    ''', (session_id, ticker_type, symbol))
                    
                    conn.commit()
                    
                    # Check if it was actually added (not a duplicate)
                    if cur.rowcount > 0:
                        logger.info(f"Added ticker {symbol} for session {session_id}")
                        return jsonify({'success': True, 'message': f'{symbol} added successfully'})
                    else:
                        return jsonify({'error': f'{symbol} is already in your list'}), 409
                        
                except Exception as e:
                    logger.error(f"Error adding ticker {symbol}: {e}")
                    return jsonify({'error': 'Failed to add ticker'}), 500
                
    except Exception as e:
        logger.error(f"Error in add_user_ticker: {e}")
        return jsonify({'error': 'Server error'}), 500

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