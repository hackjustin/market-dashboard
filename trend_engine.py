# Consensus Trend Analysis Engine
# Combines best practices from Shannon, Minervini, O'Neil, Weinstein

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from curl_cffi import requests as curl_requests

class ConsensusTrendEngine:
    """
    Multi-factor trend analysis combining methodologies from:
    - Mark Minervini (SEPA Template)
    - Brian Shannon (Multi-timeframe alignment)
    - William O'Neil (CAN SLIM)
    - Stan Weinstein (Stage Analysis)
    """
    
    def __init__(self):
        self.session = curl_requests.Session(impersonate="chrome")
    
    def get_trend_analysis(self, symbol, benchmark="SPY"):
        """
        Complete trend analysis for a symbol
        Returns trend score (0-100) and detailed breakdown
        """
        try:
            # Get data
            ticker = yf.Ticker(symbol, session=self.session)
            benchmark_ticker = yf.Ticker(benchmark, session=self.session)
            
            # Get 1 year of data for comprehensive analysis
            hist = ticker.history(period="1y")
            bench_hist = benchmark_ticker.history(period="1y")
            
            if hist.empty or len(hist) < 200:
                return {"error": f"Insufficient data for {symbol}"}
            
            # Calculate all trend components
            ma_score = self._calculate_ma_alignment(hist)
            volume_score = self._calculate_volume_strength(hist)
            momentum_score = self._calculate_momentum(hist)
            relative_strength = self._calculate_relative_strength(hist, bench_hist)
            stage_analysis = self._calculate_stage_analysis(hist)
            
            # Weighted composite score
            trend_score = (
                ma_score * 0.25 +           # 25% MA alignment
                volume_score * 0.20 +       # 20% Volume confirmation
                momentum_score * 0.20 +     # 20% Price momentum
                relative_strength * 0.20 +  # 20% Relative strength
                stage_analysis * 0.15       # 15% Stage analysis
            )
            
            return {
                "symbol": symbol,
                "trend_score": round(trend_score, 1),
                "trend_rating": self._get_trend_rating(trend_score),
                "components": {
                    "ma_alignment": round(ma_score, 1),
                    "volume_strength": round(volume_score, 1),
                    "momentum": round(momentum_score, 1),
                    "relative_strength": round(relative_strength, 1),
                    "stage_analysis": round(stage_analysis, 1)
                },
                "signals": self._generate_signals(hist),
                "risk_level": self._assess_risk(trend_score, hist),
                "timeframe_alignment": self._check_timeframe_alignment(hist)
            }
            
        except Exception as e:
            return {"error": f"Error analyzing {symbol}: {str(e)}"}
    
    def _calculate_ma_alignment(self, hist):
        """
        Minervini-style moving average alignment
        Perfect score when: Price > 50 > 150 > 200, all trending up
        """
        current_price = hist['Close'].iloc[-1]
        
        # Calculate moving averages
        ma_20 = hist['Close'].rolling(20).mean().iloc[-1]
        ma_50 = hist['Close'].rolling(50).mean().iloc[-1]
        ma_150 = hist['Close'].rolling(150).mean().iloc[-1]
        ma_200 = hist['Close'].rolling(200).mean().iloc[-1]
        
        score = 0
        
        # Price position (40 points max)
        if current_price > ma_50: score += 10
        if current_price > ma_150: score += 10
        if current_price > ma_200: score += 20
        
        # MA hierarchy (30 points max)
        if ma_50 > ma_150: score += 10
        if ma_150 > ma_200: score += 20
        
        # MA trends (30 points max)
        ma_50_trend = (ma_50 - hist['Close'].rolling(50).mean().iloc[-10]) / ma_50
        ma_200_trend = (ma_200 - hist['Close'].rolling(200).mean().iloc[-20]) / ma_200
        
        if ma_50_trend > 0: score += 15
        if ma_200_trend > 0: score += 15
        
        return min(score, 100)
    
    def _calculate_volume_strength(self, hist):
        """
        Shannon-style volume analysis
        Recent volume vs average, volume on up days vs down days
        """
        volume = hist['Volume']
        close = hist['Close']
        
        # Average volume over 50 days
        avg_volume = volume.rolling(50).mean().iloc[-1]
        recent_volume = volume.iloc[-5:].mean()
        
        score = 0
        
        # Volume expansion (40 points)
        volume_ratio = recent_volume / avg_volume
        if volume_ratio > 1.5: score += 40
        elif volume_ratio > 1.2: score += 30
        elif volume_ratio > 1.0: score += 20
        elif volume_ratio > 0.8: score += 10
        
        # Volume on up days vs down days (60 points)
        last_20 = hist.tail(20)
        up_days = last_20[last_20['Close'] > last_20['Close'].shift(1)]
        down_days = last_20[last_20['Close'] < last_20['Close'].shift(1)]
        
        if len(up_days) > 0 and len(down_days) > 0:
            up_volume = up_days['Volume'].mean()
            down_volume = down_days['Volume'].mean()
            
            if up_volume > down_volume * 1.3: score += 60
            elif up_volume > down_volume * 1.1: score += 40
            elif up_volume > down_volume: score += 20
        
        return min(score, 100)
    
    def _calculate_momentum(self, hist):
        """
        Multi-timeframe momentum analysis
        Recent performance vs different periods
        """
        current_price = hist['Close'].iloc[-1]
        
        score = 0
        
        # Performance over different periods
        periods = [5, 10, 20, 50]
        weights = [10, 15, 25, 50]  # More weight on longer-term
        
        for period, weight in zip(periods, weights):
            if len(hist) > period:
                past_price = hist['Close'].iloc[-period]
                performance = (current_price - past_price) / past_price
                
                if performance > 0.10: score += weight  # >10% gain
                elif performance > 0.05: score += weight * 0.7  # >5% gain
                elif performance > 0: score += weight * 0.4  # Positive
                elif performance > -0.05: score += weight * 0.2  # Small loss
        
        return min(score, 100)
    
    def _calculate_relative_strength(self, hist, bench_hist):
        """
        Performance vs benchmark (SPY)
        O'Neil style relative strength
        """
        if bench_hist.empty:
            return 50  # Neutral if no benchmark
        
        # Align data
        min_len = min(len(hist), len(bench_hist))
        stock_returns = hist['Close'].iloc[-min_len:].pct_change()
        bench_returns = bench_hist['Close'].iloc[-min_len:].pct_change()
        
        # Calculate relative strength over different periods
        periods = [10, 20, 50]
        weights = [20, 30, 50]
        
        score = 0
        
        for period, weight in zip(periods, weights):
            if min_len > period:
                stock_perf = (hist['Close'].iloc[-1] / hist['Close'].iloc[-period]) - 1
                bench_perf = (bench_hist['Close'].iloc[-1] / bench_hist['Close'].iloc[-period]) - 1
                
                relative_perf = stock_perf - bench_perf
                
                if relative_perf > 0.05: score += weight  # Outperforming by 5%+
                elif relative_perf > 0.02: score += weight * 0.7  # Outperforming by 2%+
                elif relative_perf > 0: score += weight * 0.5  # Outperforming
                elif relative_perf > -0.02: score += weight * 0.3  # Close to benchmark
        
        return min(score, 100)
    
    def _calculate_stage_analysis(self, hist):
        """
        Weinstein-style stage analysis
        Identify market stage: Accumulation, Advancing, Distribution, Declining
        """
        if len(hist) < 200:
            return 50
        
        current_price = hist['Close'].iloc[-1]
        ma_200 = hist['Close'].rolling(200).mean().iloc[-1]
        ma_150 = hist['Close'].rolling(150).mean().iloc[-1]
        
        # 52-week high/low analysis
        high_52w = hist['High'].iloc[-252:].max() if len(hist) >= 252 else hist['High'].max()
        low_52w = hist['Low'].iloc[-252:].min() if len(hist) >= 252 else hist['Low'].min()
        
        # Position in 52-week range
        range_position = (current_price - low_52w) / (high_52w - low_52w)
        
        score = 0
        
        # Stage identification
        if current_price > ma_200 and ma_200 > hist['Close'].rolling(200).mean().iloc[-20]:
            # Stage 2: Advancing phase
            if range_position > 0.75: score += 100  # Near highs
            elif range_position > 0.5: score += 80   # Upper half
            else: score += 60  # Above 200MA but lower in range
        elif current_price > ma_200:
            # Potential stage 1: Accumulation
            score += 40
        else:
            # Stage 3 or 4: Distribution/Decline
            score += 20 - (20 * (1 - range_position))  # Penalize lower in range
        
        return max(min(score, 100), 0)
    
    def _generate_signals(self, hist):
        """
        Generate actionable signals based on analysis
        """
        signals = []
        current_price = hist['Close'].iloc[-1]
        volume = hist['Volume'].iloc[-1]
        avg_volume = hist['Volume'].rolling(20).mean().iloc[-1]
        
        # Breakout signals
        high_20 = hist['High'].rolling(20).max().iloc[-2]  # Exclude today
        if current_price > high_20 and volume > avg_volume * 1.5:
            signals.append("BREAKOUT: New 20-day high on volume")
        
        # Moving average signals
        ma_50 = hist['Close'].rolling(50).mean().iloc[-1]
        ma_200 = hist['Close'].rolling(200).mean().iloc[-1]
        
        if current_price > ma_50 > ma_200:
            signals.append("BULLISH: Price above rising 50MA above 200MA")
        
        # Volume signals
        if volume > avg_volume * 2:
            signals.append("VOLUME: High volume day - institutional interest")
        
        return signals
    
    def _assess_risk(self, trend_score, hist):
        """
        Assess risk level based on trend strength and volatility
        """
        if trend_score >= 80:
            return "LOW"
        elif trend_score >= 60:
            return "MODERATE"
        elif trend_score >= 40:
            return "HIGH"
        else:
            return "VERY HIGH"
    
    def _check_timeframe_alignment(self, hist):
        """
        Shannon-style timeframe alignment check
        """
        current_price = hist['Close'].iloc[-1]
        
        # Short-term (20-day trend)
        ma_20 = hist['Close'].rolling(20).mean().iloc[-1]
        short_trend = "BULLISH" if current_price > ma_20 else "BEARISH"
        
        # Medium-term (50-day trend)  
        ma_50 = hist['Close'].rolling(50).mean().iloc[-1]
        medium_trend = "BULLISH" if current_price > ma_50 else "BEARISH"
        
        # Long-term (200-day trend)
        ma_200 = hist['Close'].rolling(200).mean().iloc[-1]
        long_trend = "BULLISH" if current_price > ma_200 else "BEARISH"
        
        # Check alignment
        all_bullish = all(t == "BULLISH" for t in [short_trend, medium_trend, long_trend])
        all_bearish = all(t == "BEARISH" for t in [short_trend, medium_trend, long_trend])
        
        if all_bullish:
            alignment = "FULLY ALIGNED BULLISH"
        elif all_bearish:
            alignment = "FULLY ALIGNED BEARISH"
        else:
            alignment = "MIXED SIGNALS"
        
        return {
            "short_term": short_trend,
            "medium_term": medium_trend,
            "long_term": long_trend,
            "alignment": alignment
        }
    
    def _get_trend_rating(self, score):
        """
        Convert numeric score to rating
        """
        if score >= 80:
            return "STRONG BULLISH"
        elif score >= 65:
            return "BULLISH"
        elif score >= 50:
            return "NEUTRAL BULLISH"
        elif score >= 35:
            return "NEUTRAL BEARISH"
        elif score >= 20:
            return "BEARISH"
        else:
            return "STRONG BEARISH"

# Example usage and testing
if __name__ == "__main__":
    engine = ConsensusTrendEngine()
    
    # Test with popular stocks
    symbols = ["AAPL", "NVDA", "TSLA", "MSFT", "SPY"]
    
    print("=== CONSENSUS TREND ANALYSIS ===\n")
    
    for symbol in symbols:
        print(f"Analyzing {symbol}...")
        result = engine.get_trend_analysis(symbol)
        
        if "error" in result:
            print(f"❌ {result['error']}\n")
            continue
        
        print(f"🎯 {symbol} - Trend Score: {result['trend_score']}/100")
        print(f"📊 Rating: {result['trend_rating']}")
        print(f"⚠️  Risk Level: {result['risk_level']}")
        
        print(f"\n📈 Component Breakdown:")
        for component, score in result['components'].items():
            print(f"   {component.replace('_', ' ').title()}: {score}/100")
        
        print(f"\n🎯 Timeframe Alignment:")
        alignment = result['timeframe_alignment']
        print(f"   Short-term (20d): {alignment['short_term']}")
        print(f"   Medium-term (50d): {alignment['medium_term']}")
        print(f"   Long-term (200d): {alignment['long_term']}")
        print(f"   Overall: {alignment['alignment']}")
        
        if result['signals']:
            print(f"\n🚨 Active Signals:")
            for signal in result['signals']:
                print(f"   • {signal}")
        
        print(f"\n{'-'*50}\n")
