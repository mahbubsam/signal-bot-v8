"""
╔══════════════════════════════════════════════════════════════════╗
║       SIGNAL BOT ENGINE v8 — GITHUB ACTIONS EDITION              ║
║       Runs as a scheduled job, not a persistent process          ║
║                                                                    ║
║  BASE: v6's 18-checkpoint Triple-Timeframe system (unchanged)   ║
║  Binary Options output: still included (v6 behavior, not v7)    ║
║                                                                    ║
║  CHANGE IN v8 (only execution model — logic untouched):         ║
║  ✦ scan_loop() → run_once()                                     ║
║    GitHub Actions runners are NOT long-lived processes — each   ║
║    job has a hard time limit (6h on public repos, but you       ║
║    should schedule much shorter runs). Instead of looping        ║
║    forever with time.sleep(), this version scans ALL assets     ║
║    ONE time, sends any signals found, then exits cleanly.       ║
║    A GitHub Actions cron schedule (in your workflow .yml file)  ║
║    calls this script repeatedly — e.g. every 5 minutes — which  ║
║    recreates the same "continuous scanning" behavior v6 had     ║
║    with its internal while-loop.                                ║
║  ✦ GAS_WEBHOOK_URL is hardcoded as a fallback default so the    ║
║    script works even if the GAS_URL secret isn't set — but      ║
║    using a GitHub Actions Secret is still the safer choice      ║
║    (see setup notes at the bottom of this file).                ║
║  ✦ All 18 checkpoints, accuracy scoring, SL/TP, BDT time,       ║
║    circuit breaker, cooldown — 100% identical to v6.            ║
║                                                                    ║
║  IMPORTANT — Circuit Breaker / Cooldown state persistence:       ║
║  v6's cooldown and circuit-breaker state lived in memory         ║
║  (Python dictionaries) across scan cycles because the process    ║
║  never exited. Since GitHub Actions runs this script as a fresh  ║
║  process every time, v8 now fetches that same state from a       ║
║  "BotState" tab in the Google Sheet at the START of every run    ║
║  (fetch_remote_state()) and saves it back at the END of every    ║
║  run (save_remote_state()) via two new GAS webhook actions:      ║
║  "get_state" and "save_state". This restores the exact same      ║
║  cross-run protection v6 had — a signal that fires twice in a    ║
║  row for the same symbol+direction will still trip the circuit   ║
║  breaker, even though each run is a brand-new process.           ║
║  Requires Code_v8.gs (or later) on the Google Apps Script side — ║
║  older Code.gs versions don't understand the state actions and   ║
║  will simply 404/error on them, in which case v8 logs a warning  ║
║  and proceeds with empty state for that run (fails safe, never   ║
║  blocks signal posting).                                          ║
╚══════════════════════════════════════════════════════════════════╝

GitHub Actions setup:
  1. Put this file in your repo as main.py (or update the workflow
     file's `python ...` line to match whatever you name it)
  2. requirements.txt: yfinance, pandas, requests, numpy
  3. Add a repo Secret named GAS_URL with your Apps Script URL
     (Settings → Secrets and variables → Actions → New repository secret)
  4. .github/workflows/signal-bot.yml schedules this to run on cron
"""

import os
import yfinance as yf
import pandas as pd
import numpy as np
import requests
import json
import time
import logging
from datetime import datetime, timezone, timedelta

# ──────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────
GAS_WEBHOOK_URL = os.environ.get(
    "GAS_URL",
    "https://script.google.com/macros/s/AKfycbwHR9PYafBf92YyzOOui_-oQYbEm33CXwvIYsVVGzrC-BzHQZM-VJZAu9fayTmGULfm/exec"
)

FOREX_ASSETS = [
    "EURUSD=X", "GBPUSD=X", "USDJPY=X",
    "AUDUSD=X", "USDCAD=X", "NZDUSD=X",
    "GBPJPY=X", "EURJPY=X",
]
CRYPTO_ASSETS = [
    "BTC-USD", "ETH-USD", "BNB-USD",
]
ALL_ASSETS = FOREX_ASSETS + CRYPTO_ASSETS

# ── Timeframes ────────────────────────────────────────────────────
INTERVAL_PRIMARY = "5m"
INTERVAL_HTF15    = "15m"
INTERVAL_HTF1H    = "1h"   # v6 new
PERIOD_PRIMARY   = "7d"
PERIOD_HTF15      = "10d"
PERIOD_HTF1H      = "60d"  # 1h candles need longer history for EMA200

# ── Scan timing ───────────────────────────────────────────────────
SCAN_DELAY  = 60
MAX_RETRIES = 3
RETRY_DELAY = 5

# ── Trade parameters ──────────────────────────────────────────────
FOREX_SL_PIPS  = 15
FOREX_TP_PIPS  = 40
BINARY_CANDLES = "2–3 Candles"
BINARY_TIME    = "10–15 Minutes"

# ══════════════════════════════════════════════════════════════════
# v4/v5 THRESHOLDS (unchanged)
# ══════════════════════════════════════════════════════════════════
RSI_OVERSOLD       = 25
RSI_OVERBOUGHT     = 75
ADX_MIN            = 25
BB_TOUCH_THRESHOLD = 0.35
SIGNAL_COOLDOWN    = 300

RSI_CONFIRM_CANDLES = 3
EMA_SLOPE_CANDLES   = 5
ATR_MIN_MULTIPLIER  = 0.5
ATR_MAX_MULTIPLIER  = 2.5

SESSIONS = {
    "ASIAN":    (0,  8),
    "LONDON":   (7,  16),
    "NEW_YORK": (12, 21),
}
FOREX_BEST_HOURS  = (7, 21)

MAX_SAME_DIR_SIGNALS = 2
CIRCUIT_BREAK_MINS   = 30
SIGNAL_REVERIFY_DELAY = 5

# ══════════════════════════════════════════════════════════════════
# v6 NEW THRESHOLDS
# ══════════════════════════════════════════════════════════════════

# [16] CMF — soft filter, no hard threshold, used in scoring
CMF_PERIOD = 20

# [17] Support/Resistance
SR_LOOKBACK        = 50     # candles to scan for swing highs/lows
SR_SWING_WINDOW     = 3      # candles on each side to confirm a swing point
SR_PROXIMITY_PCT    = 0.15   # price must be within 0.15% of S/R level

# [18] Candle exhaustion
EXHAUSTION_STREAK   = 4      # 4+ consecutive same-color candles = exhausted
EXHAUSTION_MIN_BODY_PCT = 0.55  # candle must have body ≥55% of range to "count"

# ── Bangladesh Time Zone (UTC+6, no DST) ──────────────────────────
BDT_OFFSET_HOURS = 6

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("SignalBot_v8")

# ── State tracking ────────────────────────────────────────────────
# v8: these three dicts work exactly as they did in v6 — the ONLY
# difference is that in v8 they get populated from Google Sheets
# at the start of run_once() and pushed back at the end, instead
# of living in memory across an infinite while-loop. No function
# that USES these dicts (circuit_breaker_check, the cooldown check
# inside analyze(), etc.) needed to change at all.
last_signal: dict          = {}
signal_direction_log: dict = {}
circuit_breaker: dict      = {}


# ══════════════════════════════════════════════════════════════════
# v8 — STATE PERSISTENCE (via Google Apps Script / Sheet)
# ══════════════════════════════════════════════════════════════════
#
# GitHub Actions runs this script as a brand-new process every time,
# so the three dicts above would normally reset to {} on every run —
# silently disabling the cooldown and circuit-breaker protections
# that v5/v6 relied on. These two functions fix that by reading/
# writing the same state to a "BotState" tab in the Google Sheet,
# through the same GAS webhook already used for posting signals.
# ══════════════════════════════════════════════════════════════════

STATE_FETCH_TIMEOUT = 15   # slightly more generous than signal POSTs


def fetch_remote_state() -> bool:
    """
    Called once at the very start of run_once(). Asks the GAS
    webhook for the last-saved state and loads it into the three
    module-level dicts (last_signal, circuit_breaker,
    signal_direction_log) so this run "remembers" what previous
    runs did.

    Returns True if state was loaded successfully, False if the
    fetch failed (in which case the bot proceeds with empty state
    — same as v6's very first run ever — rather than blocking).
    """
    global last_signal, circuit_breaker, signal_direction_log

    if not GAS_WEBHOOK_URL or "YOUR_GOOGLE" in GAS_WEBHOOK_URL:
        return False

    try:
        resp = requests.post(
            GAS_WEBHOOK_URL,
            data=json.dumps({"action": "get_state"}),
            headers={"Content-Type": "application/json"},
            timeout=STATE_FETCH_TIMEOUT,
        )
        if resp.status_code != 200:
            log.warning(f"  ⚠ State fetch: HTTP {resp.status_code} — starting with empty state")
            return False

        data  = resp.json()
        state = data.get("state", {}) or {}

        last_signal = state.get("last_signal", {}) or {}
        circuit_breaker = state.get("circuit_breaker", {}) or {}

        # signal_direction_log arrives as {"SYMBOL": [["BUY", ts], ...]}
        # — JSON turns Python tuples into lists, so convert back.
        raw_log = state.get("signal_direction_log", {}) or {}
        signal_direction_log = {
            sym: [(d, t) for (d, t) in entries]
            for sym, entries in raw_log.items()
        }

        total_cooldowns = len(last_signal)
        total_breakers  = len(circuit_breaker)
        log.info(
            f"  ✓ State loaded: {total_cooldowns} cooldown(s), "
            f"{total_breakers} active circuit-breaker(s)"
        )
        return True

    except Exception as e:
        log.warning(f"  ⚠ State fetch failed ({e}) — starting with empty state")
        return False


def save_remote_state() -> bool:
    """
    Called once at the very end of run_once(), after all assets
    have been scanned. Pushes the (possibly updated) contents of
    the three module-level dicts back to the BotState tab so the
    NEXT GitHub Actions run can pick up where this one left off.
    """
    if not GAS_WEBHOOK_URL or "YOUR_GOOGLE" in GAS_WEBHOOK_URL:
        return False

    state_payload = {
        "last_signal": last_signal,
        "circuit_breaker": circuit_breaker,
        "signal_direction_log": {
            sym: [list(entry) for entry in entries]
            for sym, entries in signal_direction_log.items()
        },
    }

    try:
        resp = requests.post(
            GAS_WEBHOOK_URL,
            data=json.dumps({"action": "save_state", "state": state_payload}),
            headers={"Content-Type": "application/json"},
            timeout=STATE_FETCH_TIMEOUT,
        )
        if resp.status_code == 200:
            log.info("  ✓ State saved for next run")
            return True
        log.warning(f"  ⚠ State save: HTTP {resp.status_code}")
        return False
    except Exception as e:
        log.warning(f"  ⚠ State save failed: {e}")
        return False


# ══════════════════════════════════════════════════════════════════
# TIME HELPERS
# ══════════════════════════════════════════════════════════════════

def utc_now():
    return datetime.now(timezone.utc)


def to_bdt_string(utc_dt: datetime) -> str:
    """
    v6: Converts a UTC datetime to Bangladesh time (UTC+6) string.
    Bangladesh does not observe daylight saving time, so this is
    a fixed +6 hour offset year-round.
    """
    bdt = utc_dt + timedelta(hours=BDT_OFFSET_HOURS)
    return bdt.strftime("%Y-%m-%d %I:%M %p") + " BDT"


# ══════════════════════════════════════════════════════════════════
# INDICATOR LIBRARY
# ══════════════════════════════════════════════════════════════════

def calc_rsi(closes: pd.Series, period: int = 14) -> pd.Series:
    delta    = closes.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs       = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def calc_ema(closes: pd.Series, span: int) -> pd.Series:
    return closes.ewm(span=span, adjust=False).mean()


def calc_adx(high, low, close, period=14):
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs()
    ], axis=1).max(axis=1)
    dm_plus  = high.diff()
    dm_minus = -low.diff()
    dm_plus  = dm_plus.where((dm_plus > dm_minus) & (dm_plus > 0), 0.0)
    dm_minus = dm_minus.where((dm_minus > dm_plus) & (dm_minus > 0), 0.0)
    atr      = tr.ewm(com=period-1, min_periods=period).mean()
    di_plus  = 100 * dm_plus.ewm(com=period-1, min_periods=period).mean() / atr
    di_minus = 100 * dm_minus.ewm(com=period-1, min_periods=period).mean() / atr
    dx       = 100 * (di_plus - di_minus).abs() / (di_plus + di_minus).replace(0, np.nan)
    adx_line = dx.ewm(com=period-1, min_periods=period).mean()
    return adx_line, di_plus, di_minus


def calc_macd(closes, fast=12, slow=26, signal=9):
    ema_fast    = calc_ema(closes, fast)
    ema_slow    = calc_ema(closes, slow)
    macd_line   = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram   = macd_line - signal_line
    return macd_line, signal_line, histogram


def calc_bollinger(closes, period=20, std_dev=2.0):
    middle = closes.rolling(window=period).mean()
    std    = closes.rolling(window=period).std()
    upper  = middle + std_dev * std
    lower  = middle - std_dev * std
    bw_pct = (upper - lower) / middle * 100
    return upper, middle, lower, bw_pct


def calc_atr(high, low, close, period=14) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(com=period-1, min_periods=period).mean()


def calc_volume_ma(volume, period=20):
    return volume.rolling(window=period).mean()


def calc_stoch_rsi(rsi_series, period=14):
    min_rsi = rsi_series.rolling(window=period).min()
    max_rsi = rsi_series.rolling(window=period).max()
    denom   = (max_rsi - min_rsi).replace(0, np.nan)
    return ((rsi_series - min_rsi) / denom) * 100


def calc_cmf(high, low, close, volume, period=CMF_PERIOD) -> pd.Series:
    """
    v6 [16]: Chaikin Money Flow — approximates institutional buying/
    selling pressure using price position within the bar + volume.

    CMF > 0  → buying pressure dominant (money flowing IN)
    CMF < 0  → selling pressure dominant (money flowing OUT)
    Range is roughly -1 to +1; values beyond ±0.1 are meaningful.

    NOTE: yfinance forex volume is tick-count, not real traded
    volume, so this is treated as a SOFT confirmation (affects
    accuracy score) rather than a hard pass/fail gate.
    """
    range_ = (high - low).replace(0, np.nan)
    mfm = ((close - low) - (high - close)) / range_   # Money Flow Multiplier
    mfv = mfm * volume                                  # Money Flow Volume
    cmf = mfv.rolling(window=period).sum() / volume.rolling(window=period).sum()
    return cmf.fillna(0)


# ══════════════════════════════════════════════════════════════════
# DATA FETCHER — with Termux-friendly retry/backoff
# ══════════════════════════════════════════════════════════════════

def fetch_ohlcv(symbol, interval, period):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            df = yf.download(symbol, period=period, interval=interval,
                             progress=False, auto_adjust=True)
            if df is None or df.empty or len(df) < 50:
                time.sleep(RETRY_DELAY)
                continue
            return df
        except Exception as e:
            log.warning(f"  {symbol} [{interval}]: attempt {attempt}: {e}")
            time.sleep(RETRY_DELAY)
    return None


# ══════════════════════════════════════════════════════════════════
# v5 SAFETY FUNCTIONS (unchanged from v5)
# ══════════════════════════════════════════════════════════════════

NEWS_BLACKOUT_WINDOWS_UTC = [
    (8,  30),   # EUR news
    (9,  30),   # GBP news
    (13, 30),   # USD news (NFP, CPI, FOMC)
    (15, 0),    # USD afternoon releases
    (18, 0),    # FOMC rate decisions
    (21, 30),   # AUD/NZD news
    (23, 50),   # BOJ / JPY decisions
]
BLACKOUT_BUFFER_MINS = 30


def is_news_blackout() -> tuple:
    now_utc  = datetime.now(timezone.utc)
    now_mins = now_utc.hour * 60 + now_utc.minute
    for (h, m) in NEWS_BLACKOUT_WINDOWS_UTC:
        news_mins = h * 60 + m
        diff = abs(now_mins - news_mins)
        diff = min(diff, 1440 - diff)
        if diff <= BLACKOUT_BUFFER_MINS:
            return True, f"{h:02d}:{m:02d} UTC ±{BLACKOUT_BUFFER_MINS}min"
    return False, ""


def rsi_sustained(rsi_series, action, candles=RSI_CONFIRM_CANDLES) -> bool:
    recent = rsi_series.iloc[-(candles + 1):-1]
    if action == "BUY":
        return all(v < 32 for v in recent)
    elif action == "SELL":
        return all(v > 68 for v in recent)
    return False


def ema_slope_valid(ema_series, action) -> bool:
    recent = ema_series.iloc[-EMA_SLOPE_CANDLES:]
    slopes = [recent.iloc[i] - recent.iloc[i-1] for i in range(1, len(recent))]
    if action == "BUY":
        return all(s > 0 for s in slopes)
    elif action == "SELL":
        return all(s < 0 for s in slopes)
    return False


def price_action_valid(open_s, high_s, low_s, close_s, action) -> tuple:
    c_o = float(open_s.iloc[-1]); c_h = float(high_s.iloc[-1])
    c_l = float(low_s.iloc[-1]);  c_c = float(close_s.iloc[-1])
    p_o = float(open_s.iloc[-2]); p_c = float(close_s.iloc[-2])
    c_body = abs(c_c - c_o); p_body = abs(p_c - p_o)
    c_range = c_h - c_l if c_h > c_l else 0.0001

    if action == "BUY":
        bullish_engulf = (c_c > c_o) and (p_c < p_o) and (c_body > p_body * 0.8)
        lower_shadow = min(c_o, c_c) - c_l
        upper_shadow = c_h - max(c_o, c_c)
        hammer = (lower_shadow >= 2 * c_body) and (upper_shadow < c_body * 0.5) and c_body > 0
        if bullish_engulf: return True, "Bullish Engulfing"
        if hammer: return True, "Hammer"
        if c_c > c_o and (c_c - c_l) / c_range >= 0.60: return True, "Strong Bullish"
        return False, "No BUY pattern"

    elif action == "SELL":
        bearish_engulf = (c_c < c_o) and (p_c > p_o) and (c_body > p_body * 0.8)
        upper_shadow = c_h - max(c_o, c_c)
        lower_shadow = min(c_o, c_c) - c_l
        shooting_star = (upper_shadow >= 2 * c_body) and (lower_shadow < c_body * 0.5) and c_body > 0
        if bearish_engulf: return True, "Bearish Engulfing"
        if shooting_star: return True, "Shooting Star"
        if c_c < c_o and (c_h - c_c) / c_range >= 0.60: return True, "Strong Bearish"
        return False, "No SELL pattern"

    return False, "Unknown"


def volatility_valid(atr_series) -> tuple:
    if len(atr_series.dropna()) < 15:
        return True, "ATR N/A"
    atr_now = float(atr_series.iloc[-1])
    atr_avg = float(atr_series.iloc[-15:].mean())
    if atr_avg == 0:
        return True, "ATR zero"
    ratio = atr_now / atr_avg
    if ratio < ATR_MIN_MULTIPLIER:
        return False, f"Volatility too low ({ratio:.2f}× avg)"
    if ratio > ATR_MAX_MULTIPLIER:
        return False, f"Volatility too high ({ratio:.2f}× avg)"
    return True, f"Volatility OK ({ratio:.2f}× avg)"


def session_valid(symbol) -> tuple:
    if "-USD" in symbol:
        return True, "Crypto (24/7)"
    now_hour = datetime.now(timezone.utc).hour
    in_window = FOREX_BEST_HOURS[0] <= now_hour < FOREX_BEST_HOURS[1]
    if not in_window:
        return False, f"Outside forex session (UTC {now_hour}:00)"
    active_sessions = []
    for name, (start, end) in SESSIONS.items():
        if start <= now_hour < end:
            active_sessions.append(name)
    session_str = "+".join(active_sessions) if active_sessions else "Transition"
    return True, session_str


def circuit_breaker_check(symbol, action) -> tuple:
    now_ts = time.time()
    if symbol in circuit_breaker:
        pause_until = circuit_breaker[symbol]
        if now_ts < pause_until:
            remaining = int((pause_until - now_ts) / 60)
            return False, f"Circuit breaker active ({remaining}min remaining)"
        else:
            del circuit_breaker[symbol]

    log_key = symbol
    if log_key not in signal_direction_log:
        signal_direction_log[log_key] = []
    cutoff = now_ts - 7200
    signal_direction_log[log_key] = [
        (d, t) for (d, t) in signal_direction_log[log_key] if t > cutoff
    ]
    recent_same = [
        (d, t) for (d, t) in signal_direction_log[log_key]
        if d == action and (now_ts - t) < 3600
    ]
    if len(recent_same) >= MAX_SAME_DIR_SIGNALS:
        pause_until = now_ts + (CIRCUIT_BREAK_MINS * 60)
        circuit_breaker[symbol] = pause_until
        log.warning(f"  ⚡ CIRCUIT BREAKER: {symbol} paused {CIRCUIT_BREAK_MINS}min")
        return False, f"Circuit breaker triggered ({CIRCUIT_BREAK_MINS}min pause)"

    signal_direction_log[log_key].append((action, now_ts))
    return True, "OK"


def reverify_signal(symbol, action, original_rsi) -> bool:
    time.sleep(SIGNAL_REVERIFY_DELAY)
    df = fetch_ohlcv(symbol, INTERVAL_PRIMARY, PERIOD_PRIMARY)
    if df is None:
        return True
    close = df["Close"].squeeze()
    rsi_new = calc_rsi(close, 14)
    r_new = float(rsi_new.iloc[-1])
    if action == "BUY" and r_new > 35:
        log.info(f"  SB8: {symbol} BUY signal stale (RSI now {r_new:.1f})")
        return False
    if action == "SELL" and r_new < 65:
        log.info(f"  SB8: {symbol} SELL signal stale (RSI now {r_new:.1f})")
        return False
    return True


# ══════════════════════════════════════════════════════════════════
# v6 [15] — TRIPLE TIMEFRAME TREND (1h + 15m + 5m)
# ══════════════════════════════════════════════════════════════════

def get_tf_trend(symbol: str, interval: str, period: str) -> str | None:
    """
    Generic trend-getter for any timeframe using EMA50/EMA200 stack.
    Used for both 15m (existing) and 1h (new in v6).
    """
    df = fetch_ohlcv(symbol, interval, period)
    if df is None:
        return None
    close = df["Close"].squeeze()
    if len(close) < 210:
        return None   # not enough bars to compute EMA200 reliably
    e50h  = calc_ema(close, 50)
    e200h = calc_ema(close, 200)
    c    = float(close.iloc[-1])
    e50  = float(e50h.iloc[-1])
    e200 = float(e200h.iloc[-1])
    if c > e50 > e200: return "UP"
    if c < e50 < e200: return "DOWN"
    return None


def triple_tf_agreement(action: str, trend_5m_ok: bool,
                        trend_15m: str, trend_1h: str) -> tuple:
    """
    v6 [15]: All three timeframes must point the same direction.
    This is the single highest-value addition — when 1h, 15m and
    5m all agree, historical win-rate on this kind of setup is
    meaningfully higher than any single-timeframe signal.
    """
    wanted = "UP" if action == "BUY" else "DOWN"

    agree_15m = (trend_15m == wanted)
    agree_1h  = (trend_1h == wanted)

    if agree_15m and agree_1h:
        return True, f"1h={trend_1h} 15m={trend_15m} 5m={action} ✓ ALIGNED"
    else:
        return False, f"1h={trend_1h or 'FLAT'} 15m={trend_15m or 'FLAT'} — not aligned"


# ══════════════════════════════════════════════════════════════════
# v6 [17] — SUPPORT / RESISTANCE PROXIMITY
# ══════════════════════════════════════════════════════════════════

def find_swing_points(high: pd.Series, low: pd.Series,
                      lookback=SR_LOOKBACK, window=SR_SWING_WINDOW):
    """
    Finds recent swing-high (resistance) and swing-low (support)
    prices using a simple local-extremum method: a candle is a
    swing high if its High is greater than `window` candles on
    both sides, and similarly for swing lows.
    """
    h = high.iloc[-lookback:].reset_index(drop=True)
    l = low.iloc[-lookback:].reset_index(drop=True)

    swing_highs = []
    swing_lows  = []

    for i in range(window, len(h) - window):
        segment_h = h.iloc[i-window:i+window+1]
        segment_l = l.iloc[i-window:i+window+1]
        if h.iloc[i] == segment_h.max():
            swing_highs.append(float(h.iloc[i]))
        if l.iloc[i] == segment_l.min():
            swing_lows.append(float(l.iloc[i]))

    return swing_highs, swing_lows


def support_resistance_valid(high: pd.Series, low: pd.Series,
                             close: pd.Series, action: str) -> tuple:
    """
    v6 [17]: BUY signals are only allowed when price is trading
    very close to a recent swing-low (support). SELL signals are
    only allowed near a recent swing-high (resistance).

    This stops the classic mistake of buying near the TOP of a
    move because RSI/MACD/etc happened to align — price context
    (where are we relative to recent structure) matters too.
    """
    swing_highs, swing_lows = find_swing_points(high, low)
    c0 = float(close.iloc[-1])

    if action == "BUY":
        if not swing_lows:
            return False, "No support levels found"
        nearest_support = min(swing_lows, key=lambda lvl: abs(c0 - lvl))
        dist_pct = abs(c0 - nearest_support) / c0 * 100
        if dist_pct <= SR_PROXIMITY_PCT:
            return True, f"Near support {nearest_support:.5f} ({dist_pct:.3f}%)"
        return False, f"Too far from support ({dist_pct:.3f}% away, need ≤{SR_PROXIMITY_PCT}%)"

    elif action == "SELL":
        if not swing_highs:
            return False, "No resistance levels found"
        nearest_resistance = min(swing_highs, key=lambda lvl: abs(c0 - lvl))
        dist_pct = abs(c0 - nearest_resistance) / c0 * 100
        if dist_pct <= SR_PROXIMITY_PCT:
            return True, f"Near resistance {nearest_resistance:.5f} ({dist_pct:.3f}%)"
        return False, f"Too far from resistance ({dist_pct:.3f}% away, need ≤{SR_PROXIMITY_PCT}%)"

    return False, "Unknown action"


# ══════════════════════════════════════════════════════════════════
# v6 [18] — CANDLE EXHAUSTION GUARD
# ══════════════════════════════════════════════════════════════════

def exhaustion_check(open_s: pd.Series, high_s: pd.Series,
                     low_s: pd.Series, close_s: pd.Series,
                     action: str, streak=EXHAUSTION_STREAK) -> tuple:
    """
    v6 [18]: If the last N candles (default 4) are all strong
    same-direction candles (body ≥55% of range), the move is
    likely overextended and due for a pullback/reversal.

    Blocks BUY after N consecutive strong green candles.
    Blocks SELL after N consecutive strong red candles.
    """
    o = open_s.iloc[-streak:].values
    h = high_s.iloc[-streak:].values
    l = low_s.iloc[-streak:].values
    c = close_s.iloc[-streak:].values

    strong_green_count = 0
    strong_red_count   = 0

    for i in range(streak):
        body  = abs(c[i] - o[i])
        rng   = h[i] - l[i] if h[i] > l[i] else 0.0001
        body_pct = body / rng
        if body_pct < EXHAUSTION_MIN_BODY_PCT:
            continue  # weak/doji candle breaks the streak significance
        if c[i] > o[i]:
            strong_green_count += 1
        elif c[i] < o[i]:
            strong_red_count += 1

    if action == "BUY" and strong_green_count >= streak:
        return False, f"Exhaustion: {strong_green_count} consecutive strong green candles"
    if action == "SELL" and strong_red_count >= streak:
        return False, f"Exhaustion: {strong_red_count} consecutive strong red candles"

    return True, "No exhaustion detected"


# ══════════════════════════════════════════════════════════════════
# ACCURACY SCORER — v6 (adds CMF soft bonus)
# ══════════════════════════════════════════════════════════════════

def compute_accuracy(rsi_val, stoch_rsi, ema50, ema200, adx,
                     dip, dim, hist, hist_prev, vol, vol_ma_val,
                     action, pa_pattern, atr_ratio, session_str,
                     cmf_val, tf_aligned) -> str:
    """
    Base 85 (all v4 layers passed). Bonus up to 100.

    v6 adds:
      - CMF soft bonus          +0–3  (money flow agrees with direction)
      - Triple-TF alignment     +2    (1h+15m+5m all agree — big deal)
    """
    score = 85.0

    # ── v4 bonuses ─────────────────────────────────────────────────
    rsi_depth = abs(rsi_val - 50)
    if   rsi_depth >= 28: score += 4
    elif rsi_depth >= 26: score += 3
    elif rsi_depth >= 24: score += 2
    elif rsi_depth >= 22: score += 1

    if stoch_rsi < 10 or stoch_rsi > 90: score += 2
    elif stoch_rsi < 15 or stoch_rsi > 85: score += 1

    if   adx >= 40: score += 3
    elif adx >= 35: score += 2
    elif adx >= 30: score += 1

    di_gap = abs(dip - dim)
    if   di_gap >= 20: score += 2
    elif di_gap >= 12: score += 1

    hist_growth = abs(hist) - abs(hist_prev)
    if hist_growth > 0 and abs(hist) > 0.0001: score += 2
    elif hist_growth > 0: score += 1

    if vol_ma_val > 0 and vol > vol_ma_val * 1.5: score += 2
    elif vol_ma_val > 0 and vol > vol_ma_val * 1.2: score += 1

    # ── v5 bonuses ─────────────────────────────────────────────────
    if "Engulfing" in pa_pattern: score += 3
    elif "Hammer" in pa_pattern or "Shooting" in pa_pattern: score += 2
    elif "Strong" in pa_pattern: score += 1

    if 0.8 <= atr_ratio <= 1.5: score += 2
    elif 0.5 <= atr_ratio <= 2.0: score += 1

    if "LONDON" in session_str or "NEW_YORK" in session_str:
        score += 2
        if "LONDON" in session_str and "NEW_YORK" in session_str:
            score += 1

    # ── v6 [16] CMF soft bonus ────────────────────────────────────
    # Only ADDS confidence, never subtracts — because tick-volume
    # based CMF is not reliable enough to penalize a signal for.
    if action == "BUY" and cmf_val > 0.05:
        score += 3
    elif action == "BUY" and cmf_val > 0:
        score += 1
    elif action == "SELL" and cmf_val < -0.05:
        score += 3
    elif action == "SELL" and cmf_val < 0:
        score += 1

    # ── v6 [15] Triple timeframe alignment bonus ──────────────────
    if tf_aligned:
        score += 2

    score = min(round(score), 100)
    return f"{score}%"


# ══════════════════════════════════════════════════════════════════
# MAIN ANALYSIS — v6: 18-CHECKPOINT SYSTEM
# ══════════════════════════════════════════════════════════════════

def analyze(symbol: str) -> dict | None:
    """
    ╔═══════════════════════════════════════════════════════════════╗
    ║  18-CHECKPOINT SYSTEM — v6                                    ║
    ╠═══════════════════════════════════════════════════════════════╣
    ║  [1-8]   v4 base layers (EMA, RSI, ADX, DI, Vol, MACD, BB,   ║
    ║          15m HTF)                                             ║
    ║  [9-14]  v5 safety layers (News, RSI-Sustain, EMA-Slope,     ║
    ║          Price Action, ATR, Session)                          ║
    ║  [15]    v6 — 1h Trend Agreement (NEW)                        ║
    ║  [16]    v6 — CMF soft filter (scoring only, not hard block) ║
    ║  [17]    v6 — Support/Resistance Proximity (NEW)              ║
    ║  [18]    v6 — Candle Exhaustion Guard (NEW)                   ║
    ║  [CB]    Circuit Breaker (post-signal)                        ║
    ║  [RV]    Re-verify (post-signal)                              ║
    ╚═══════════════════════════════════════════════════════════════╝
    """

    # ── Pre-flight: session & news (cheapest checks first) ────────
    sess_ok, sess_str = session_valid(symbol)
    if not sess_ok:
        log.info(f"    [14] ✗ {sess_str}")
        return None

    news_ok, news_str = is_news_blackout()
    if news_ok:
        log.info(f"    [9] ✗ News blackout: {news_str}")
        return None

    # ── Fetch primary (5m) data ────────────────────────────────────
    df = fetch_ohlcv(symbol, INTERVAL_PRIMARY, PERIOD_PRIMARY)
    if df is None:
        return None

    close  = df["Close"].squeeze()
    high   = df["High"].squeeze()
    low    = df["Low"].squeeze()
    open_  = df["Open"].squeeze()
    volume = df["Volume"].squeeze()

    if len(close) < 220:
        log.info(f"    Insufficient data ({len(close)} bars)")
        return None

    # ── Calculate all indicators ────────────────────────────────────
    rsi             = calc_rsi(close, 14)
    stoch_rsi       = calc_stoch_rsi(rsi, 14)
    ema50           = calc_ema(close, 50)
    ema200          = calc_ema(close, 200)
    adx, di_p, di_m = calc_adx(high, low, close, 14)
    macd, sig, hist = calc_macd(close)
    bb_up, bb_mid, bb_lo, bb_bw = calc_bollinger(close, 20, 2.0)
    atr             = calc_atr(high, low, close, 14)
    vol_ma          = calc_volume_ma(volume, 20)
    cmf             = calc_cmf(high, low, close, volume, CMF_PERIOD)

    # ── Latest values ────────────────────────────────────────────────
    c0    = float(close.iloc[-1])
    o0    = float(open_.iloc[-1])
    r0    = float(rsi.iloc[-1])
    r1    = float(rsi.iloc[-2])
    sr0   = float(stoch_rsi.iloc[-1]) if not np.isnan(stoch_rsi.iloc[-1]) else 50.0
    e50   = float(ema50.iloc[-1])
    e200  = float(ema200.iloc[-1])
    adx0  = float(adx.iloc[-1])
    dip0  = float(di_p.iloc[-1])
    dim0  = float(di_m.iloc[-1])
    hist0 = float(hist.iloc[-1])  if not np.isnan(hist.iloc[-1])  else 0.0
    hist1 = float(hist.iloc[-2])  if not np.isnan(hist.iloc[-2])  else 0.0
    bbu0  = float(bb_up.iloc[-1]) if not np.isnan(bb_up.iloc[-1]) else c0*1.02
    bbl0  = float(bb_lo.iloc[-1]) if not np.isnan(bb_lo.iloc[-1]) else c0*0.98
    atr0  = float(atr.iloc[-1])   if not np.isnan(atr.iloc[-1])   else 0.0
    atr_avg = float(atr.iloc[-15:].mean()) if len(atr.dropna()) >= 15 else atr0
    atr_ratio = atr0 / atr_avg if atr_avg > 0 else 1.0
    v0    = float(volume.iloc[-1])
    vm0   = float(vol_ma.iloc[-1])
    cmf0  = float(cmf.iloc[-1]) if not np.isnan(cmf.iloc[-1]) else 0.0
    band_width = bbu0 - bbl0

    # ── [1] EMA Trend Stack ──────────────────────────────────────────
    uptrend   = (c0 > e50)  and (e50  > e200)
    downtrend = (c0 < e50)  and (e50  < e200)

    # ── [2] RSI Crossover ────────────────────────────────────────────
    rsi_buy  = (r1 >= RSI_OVERSOLD)   and (r0 < RSI_OVERSOLD)
    rsi_sell = (r1 <= RSI_OVERBOUGHT) and (r0 > RSI_OVERBOUGHT)

    # ── [3] ADX Strength ──────────────────────────────────────────────
    strong_trend = adx0 >= ADX_MIN

    # ── [4] DI Direction ──────────────────────────────────────────────
    di_bullish = dip0 > dim0
    di_bearish = dim0 > dip0

    # ── [5] Volume Surge ──────────────────────────────────────────────
    vol_confirm = (v0 > vm0 * 1.10) if vm0 > 0 else True

    # ── [6] MACD Histogram ──────────────────────────────────────────
    macd_buy  = (hist0 > 0) and (hist0 > hist1)
    macd_sell = (hist0 < 0) and (hist0 < hist1)

    # ── [7] Bollinger Band Touch ──────────────────────────────────────
    if band_width > 0:
        dist_from_lower = (c0 - bbl0) / band_width
        dist_from_upper = (bbu0 - c0) / band_width
        bb_buy_zone  = dist_from_lower <= BB_TOUCH_THRESHOLD
        bb_sell_zone = dist_from_upper <= BB_TOUCH_THRESHOLD
    else:
        bb_buy_zone = bb_sell_zone = False

    # ── [8] HTF 15m Agreement ─────────────────────────────────────────
    htf_trend_15m  = get_tf_trend(symbol, INTERVAL_HTF15, PERIOD_HTF15)
    htf_agree_buy  = htf_trend_15m == "UP"
    htf_agree_sell = htf_trend_15m == "DOWN"

    # ── Determine preliminary action (v4 base — 8 checks) ─────────────
    v4_buy_conditions = [
        uptrend, rsi_buy, strong_trend, di_bullish,
        vol_confirm, macd_buy, bb_buy_zone, htf_agree_buy
    ]
    v4_sell_conditions = [
        downtrend, rsi_sell, strong_trend, di_bearish,
        vol_confirm, macd_sell, bb_sell_zone, htf_agree_sell
    ]

    if all(v4_buy_conditions):
        action = "BUY"
    elif all(v4_sell_conditions):
        action = "SELL"
    else:
        passed = sum(v4_buy_conditions)
        if passed >= 5:
            conds = ["EMA","RSI","ADX","DI","VOL","MACD","BB","HTF15"]
            failed = [conds[i] for i,v in enumerate(v4_buy_conditions) if not v]
            log.info(f"    v4 BUY near-miss ({passed}/8) — failed: {failed}")
        return None

    log.info(f"    ✓ [1-8] v4 base passed ({action}) → running v5+v6 checks…")

    # ── [10] RSI Sustained (v5) ───────────────────────────────────────
    if not rsi_sustained(rsi, action, RSI_CONFIRM_CANDLES):
        log.info(f"    [10] ✗ RSI not sustained for {RSI_CONFIRM_CANDLES} candles")
        return None

    # ── [11] EMA Slope Acceleration (v5) ──────────────────────────────
    if not ema_slope_valid(ema50, action):
        log.info(f"    [11] ✗ EMA50 slope not accelerating for {action}")
        return None

    # ── [12] Price Action Confirmation (v5) ───────────────────────────
    pa_ok, pa_pattern = price_action_valid(open_, high, low, close, action)
    if not pa_ok:
        log.info(f"    [12] ✗ Price action: {pa_pattern}")
        return None
    log.info(f"    [12] ✓ Price action: {pa_pattern}")

    # ── [13] ATR Volatility Gate (v5) ─────────────────────────────────
    vol_gate_ok, vol_gate_str = volatility_valid(atr)
    if not vol_gate_ok:
        log.info(f"    [13] ✗ {vol_gate_str}")
        return None
    log.info(f"    [13] ✓ {vol_gate_str}")

    # ── [15] v6 — 1h Trend Agreement ──────────────────────────────────
    htf_trend_1h = get_tf_trend(symbol, INTERVAL_HTF1H, PERIOD_HTF1H)
    tf_aligned, tf_str = triple_tf_agreement(action, True, htf_trend_15m, htf_trend_1h)
    if not tf_aligned:
        log.info(f"    [15] ✗ Triple-TF: {tf_str}")
        return None
    log.info(f"    [15] ✓ Triple-TF: {tf_str}")

    # ── [17] v6 — Support/Resistance Proximity ────────────────────────
    sr_ok, sr_str = support_resistance_valid(high, low, close, action)
    if not sr_ok:
        log.info(f"    [17] ✗ S/R: {sr_str}")
        return None
    log.info(f"    [17] ✓ S/R: {sr_str}")

    # ── [18] v6 — Candle Exhaustion Guard ─────────────────────────────
    exh_ok, exh_str = exhaustion_check(open_, high, low, close, action)
    if not exh_ok:
        log.info(f"    [18] ✗ {exh_str}")
        return None
    log.info(f"    [18] ✓ {exh_str}")

    # ── Circuit Breaker ─────────────────────────────────────────────
    cb_ok, cb_str = circuit_breaker_check(symbol, action)
    if not cb_ok:
        log.info(f"    [CB] ✗ {cb_str}")
        return None

    # ── Cooldown ───────────────────────────────────────────────────
    ck     = f"{symbol}_{action}"
    now_ts = time.time()
    if ck in last_signal and (now_ts - last_signal[ck]) < SIGNAL_COOLDOWN:
        log.info(f"    Cooldown active for {action}")
        return None
    last_signal[ck] = now_ts

    # ── [RV] Re-verify (v5) ───────────────────────────────────────────
    log.info(f"    [RV] Re-verifying in {SIGNAL_REVERIFY_DELAY}s…")
    if not reverify_signal(symbol, action, r0):
        return None
    log.info(f"    [RV] ✓ Signal still valid after re-verify")

    # ── Compute accuracy (includes v6 CMF + TF-alignment bonus) ───────
    accuracy = compute_accuracy(
        rsi_val=r0, stoch_rsi=sr0,
        ema50=e50, ema200=e200, adx=adx0,
        dip=dip0, dim=dim0,
        hist=hist0, hist_prev=hist1,
        vol=v0, vol_ma_val=vm0,
        action=action,
        pa_pattern=pa_pattern,
        atr_ratio=atr_ratio,
        session_str=sess_str,
        cmf_val=cmf0,
        tf_aligned=tf_aligned,
    )

    return {
        "symbol":       symbol,
        "action":       action,
        "price":        round(c0, 5),
        "rsi":          round(r0, 2),
        "stoch_rsi":    round(sr0, 2),
        "ema50":        round(e50, 5),
        "ema200":       round(e200, 5),
        "adx":          round(adx0, 2),
        "macd_hist":    round(hist0, 6),
        "cmf":          round(cmf0, 4),
        "htf_15m":      htf_trend_15m or "FLAT",
        "htf_1h":       htf_trend_1h or "FLAT",
        "tf_summary":   tf_str,
        "sr_info":      sr_str,
        "pa_pattern":   pa_pattern,
        "atr_ratio":    round(atr_ratio, 2),
        "session":      sess_str,
        "accuracy":     accuracy,
        "checks":       "18/18",
    }


# ══════════════════════════════════════════════════════════════════
# PIP / SL / TP
# ══════════════════════════════════════════════════════════════════

def pip_value(symbol):
    if "JPY" in symbol:  return 0.01
    if "-USD" in symbol: return 1.0
    return 0.0001


def calc_sl_tp(symbol, price, action):
    pip  = pip_value(symbol)
    sl_p = FOREX_SL_PIPS * pip
    tp_p = FOREX_TP_PIPS * pip
    if action == "BUY":
        sl = round(price - sl_p, 5)
        tp = round(price + tp_p, 5)
    else:
        sl = round(price + sl_p, 5)
        tp = round(price - tp_p, 5)
    return sl, tp, f"1:{round(FOREX_TP_PIPS/FOREX_SL_PIPS,2)}"


# ══════════════════════════════════════════════════════════════════
# PAYLOAD BUILDER — includes v6 BDT timestamp
# ══════════════════════════════════════════════════════════════════

def build_payloads(sig):
    symbol   = sig["symbol"]
    action   = sig["action"]
    price    = sig["price"]
    accuracy = sig["accuracy"]

    now_utc  = utc_now()
    ts_utc   = now_utc.strftime("%Y-%m-%d %H:%M UTC")
    ts_bdt   = to_bdt_string(now_utc)          # v6 new
    ts_combo = f"{ts_utc} | {ts_bdt}"          # shown in Timestamp field

    sl, tp, rr = calc_sl_tp(symbol, price, action)

    detail = (
        f"SL:{sl} TP:{tp} | {sig['checks']} checks | "
        f"1h:{sig['htf_1h']} 15m:{sig['htf_15m']} | "
        f"PA:{sig['pa_pattern']} | Session:{sig['session']} | "
        f"ATR:{sig['atr_ratio']}× | CMF:{sig['cmf']:+.3f} | "
        f"{sig['sr_info']} | {ts_bdt}"
    )

    forex_row = {
        "Timestamp":   ts_combo,
        "Market Type": "FOREX PIP ENGINE",
        "Asset":       symbol,
        "Action":      action,
        "Entry Price": str(price),
        "Stop Loss":   str(sl),
        "Take Profit": str(tp),
        "Risk:Reward": rr,
        "RSI":         str(sig["rsi"]),
        "ADX":         str(sig["adx"]),
        "EMA50":       str(sig["ema50"]),
        "EMA200":      str(sig["ema200"]),
        "Accuracy":    accuracy,
        "Detail 1":    detail,
        "SL/TP":       f"SL {sl} / TP {tp}",
    }

    binary_row = {
        "Timestamp":        ts_combo,
        "Market Type":      "BINARY OPTIONS DIRECT",
        "Asset":            symbol,
        "Action":           action,
        "Entry Price":      str(price),
        "SL/TP":            "N/A (Fixed Risk)",
        "Expiry (Candles)": BINARY_CANDLES,
        "Expiry (Time)":    BINARY_TIME,
        "RSI":              str(sig["rsi"]),
        "ADX":              str(sig["adx"]),
        "EMA50":            str(sig["ema50"]),
        "EMA200":           str(sig["ema200"]),
        "Accuracy":         accuracy,
        "Detail 1":         (
            f"Expiry:{BINARY_TIME} | {sig['checks']} checks | "
            f"1h:{sig['htf_1h']} 15m:{sig['htf_15m']} | "
            f"PA:{sig['pa_pattern']} | CMF:{sig['cmf']:+.3f} | {ts_bdt}"
        ),
        "Stop Loss":        "N/A",
        "Take Profit":      "N/A",
        "Risk:Reward":      "Fixed",
    }

    return [forex_row, binary_row]


# ══════════════════════════════════════════════════════════════════
# WEBHOOK SENDER
# ══════════════════════════════════════════════════════════════════

def send_to_gas(rows):
    headers = {"Content-Type": "application/json"}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                GAS_WEBHOOK_URL,
                data=json.dumps({"rows": rows}),
                headers=headers,
                timeout=15,   # slightly higher timeout for mobile data
            )
            if resp.status_code == 200:
                log.info(f"  ✓ Webhook OK ({len(rows)} rows)")
                return True
            log.warning(f"  Webhook attempt {attempt}: HTTP {resp.status_code}")
        except Exception as e:
            log.warning(f"  Webhook attempt {attempt}: {e}")
        time.sleep(RETRY_DELAY)
    log.error("  ✗ Webhook failed")
    return False


# ══════════════════════════════════════════════════════════════════
# MAIN — v8: SINGLE-RUN MODE (for GitHub Actions cron scheduling)
# ══════════════════════════════════════════════════════════════════
#
# v6's scan_loop() looped forever with time.sleep(SCAN_DELAY) between
# cycles, because it ran on a persistent server/phone. GitHub Actions
# runners are ephemeral — the job starts, must finish, then the VM is
# torn down. So v8 scans every asset ONE time and exits. Your GitHub
# Actions workflow (.yml) re-triggers this script on a cron schedule
# (e.g. "*/5 * * * *" for every 5 minutes) to get the same continuous
# coverage v6 had with its internal loop.
#
# SCAN_DELAY is no longer used to sleep between cycles — that job now
# belongs to the GitHub Actions cron schedule instead. It's left
# defined above (unused) so nothing else in the file needs to change.
# ══════════════════════════════════════════════════════════════════

def run_once():
    log.info("═" * 70)
    log.info("  SIGNAL BOT v8.0 — GITHUB ACTIONS EDITION")
    log.info("  18-Checkpoint System | Single-Run Mode (triggered by cron)")
    log.info(f"  Assets  : {len(ALL_ASSETS)} pairs")
    log.info(f"  Frames  : 5m (entry) + 15m (HTF) + 1h (macro trend)")
    log.info("  Filters : 1h-Trend✦ CMF(soft)✦ Support/Resistance✦ Exhaustion-Guard✦ BDT-Time")
    log.info("═" * 70)

    if not GAS_WEBHOOK_URL or "YOUR_GOOGLE" in GAS_WEBHOOK_URL:
        log.error("  ✗ GAS_URL not set and no valid fallback found!")
        log.error("  ✗ GitHub Actions: add a repo Secret named GAS_URL")
        log.error("  ✗ (Settings → Secrets and variables → Actions → New repository secret)")
        return

    # ── v8: load cooldown/circuit-breaker state from previous runs ───
    log.info("  Loading state from previous run...")
    fetch_remote_state()

    now_utc_dt  = utc_now()
    now_utc_str = now_utc_dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    now_bdt_str = to_bdt_string(now_utc_dt)

    news_block, news_info = is_news_blackout()
    if news_block:
        log.info(f"\n[RUN] {now_utc_str} ({now_bdt_str})")
        log.info(f"  ⛔ NEWS BLACKOUT: {news_info} — skipping this run entirely")
        log.info("  (The next scheduled GitHub Actions run will try again)")
        # No scanning happened, so state didn't change — but save anyway
        # in case this is the very first run and BotState tab needs init.
        save_remote_state()
        return

    log.info(f"\n{'═'*70}")
    log.info(f"[RUN] {now_utc_str}  |  {now_bdt_str}")
    log.info(f"{'═'*70}")

    run_start = time.time()
    found = 0

    for symbol in ALL_ASSETS:
        log.info(f"\n  ▶ {symbol}")
        try:
            sig = analyze(symbol)
            if sig:
                log.info(
                    f"  ★★★★ SIGNAL → {symbol} {sig['action']} @ {sig['price']}"
                    f"\n       RSI={sig['rsi']} ADX={sig['adx']} CMF={sig['cmf']:+.3f}"
                    f"\n       1h={sig['htf_1h']} 15m={sig['htf_15m']}"
                    f"\n       PA={sig['pa_pattern']} | {sig['sr_info']}"
                    f"\n       Session={sig['session']} Acc={sig['accuracy']}"
                    f"\n       [{sig['checks']} checkpoints passed]"
                )
                rows = build_payloads(sig)
                send_to_gas(rows)
                found += 1
            else:
                log.info(f"    · No signal")
        except Exception as e:
            log.error(f"  ✗ {symbol}: error: {e}")
        time.sleep(2.0)

    elapsed = time.time() - run_start
    log.info(
        f"\n[RUN DONE] {found} signal(s) | {elapsed:.1f}s elapsed"
        f"\n  Next scan will happen on the next GitHub Actions cron trigger.\n"
    )

    # ── v8: persist cooldown/circuit-breaker state for next run ──────
    log.info("  Saving state for next run...")
    save_remote_state()


if __name__ == "__main__":
    run_once()
