"""
MarketWatch VSE  —  Advanced Day Trader
========================================
Upgraded algorithm with:
  • 3 entry signals: gap at open, intraday breakout, momentum continuation
  • Better stock filtering: min price $5, min dollar volume $5M
  • Up to 2 simultaneous positions
  • Dynamic position sizing based on signal strength
  • Trailing stop to lock in gains
  • Scale out: sell 50% at +3%, let rest run to +7%
  • Scans every 10 seconds all day
  • 3:45 PM fade scan (short stocks up 100%+)
  • 3:55 PM force close everything

Run:
    python3 algo.py
"""

import json
import os
import sys
import time
import subprocess
from datetime import datetime, date
from collections import defaultdict

import yfinance as yf
import pandas as pd

# ── Config ─────────────────────────────────────────────────────────────────────

MW_GAME          = "yuse-spring-2026-stock-market-competition-"
MAX_POSITIONS    = 2          # max simultaneous open positions
TOTAL_CAPITAL    = 400_000    # total capital to deploy
CHECK_INTERVAL   = 10         # seconds between scans

# Entry filters
MIN_PRICE        = 5.0        # minimum stock price
MIN_DOLLAR_VOL   = 5_000_000  # minimum $ traded today
MIN_RELVOL       = 1.5        # minimum relative volume
MIN_CHANGE_PCT   = 3.0        # minimum % move to consider

# Exit thresholds
SCALE_OUT_PCT    = 3.0        # sell 50% of position here
FULL_TARGET_PCT  = 7.0        # sell remaining 50% here
STOP_PCT         = -3.0       # hard stop loss
TRAIL_PCT        = 2.0        # trailing stop — locks in gains (trails 2% below peak)

# Fade (pre-close)
FADE_MIN_PCT     = 100.0
FADE_TIME        = (15, 45)
CLOSE_TIME       = (15, 55)


# ── AppleScript / Safari ───────────────────────────────────────────────────────

def run_applescript(script: str) -> str:
    r = subprocess.run(["osascript", "-e", script], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"AppleScript: {r.stderr.strip()}")
    return r.stdout.strip()


def safari_js(js: str) -> str:
    escaped = js.replace("\\", "\\\\").replace('"', '\\"')
    script = f'''tell application "Safari"
        tell front document
            do JavaScript "{escaped}"
        end tell
    end tell'''
    return run_applescript(script)


def open_url(url: str):
    run_applescript(f'tell application "Safari" to open location "{url}"')
    time.sleep(4)


# ── Market data ────────────────────────────────────────────────────────────────

def get_quote(ticker: str) -> dict | None:
    try:
        t           = yf.Ticker(ticker)
        fi          = t.fast_info
        prev        = getattr(fi, "previous_close",             None)
        price       = getattr(fi, "last_price",                 None)
        last_vol    = getattr(fi, "last_volume",                None)
        avg_vol     = getattr(fi, "three_month_average_volume", None)
        day_high    = getattr(fi, "day_high",                   None)
        day_low     = getattr(fi, "day_low",                    None)
        if not prev or not price or prev <= 0:
            return None
        change      = (price - prev) / prev * 100
        rel_vol     = (last_vol / avg_vol) if avg_vol and last_vol else 0.0
        dollar_vol  = (last_vol or 0) * price
        return {
            "ticker":     ticker,
            "price":      price,
            "prev":       prev,
            "change":     change,
            "rel_vol":    rel_vol,
            "dollar_vol": dollar_vol,
            "day_high":   day_high or price,
            "day_low":    day_low  or price,
            "last_vol":   last_vol or 0,
        }
    except Exception:
        return None


def get_universe() -> list[str]:
    sys.path.insert(0, os.path.dirname(__file__))
    import backtest as b
    live = b.fetch_live_movers(b.BT)
    return list(dict.fromkeys(b.BT["gap_universe"] + live))


def passes_filters(q: dict) -> bool:
    return (
        q["price"]      >= MIN_PRICE       and
        q["dollar_vol"] >= MIN_DOLLAR_VOL  and
        q["rel_vol"]    >= MIN_RELVOL
    )


# ── Technical analysis (runs on shortlisted candidates only) ──────────────────

def analyze_technicals(ticker: str) -> dict:
    """
    Fetch 1-min intraday candles and return a dict of technical factors.
    Returns a neutral dict (no boost, no block) if data unavailable.
    """
    neutral = {
        "vwap": None, "above_vwap": None,
        "trend": "FLAT",
        "vol_accel": 1.0,
        "pct_off_high": 5.0, "pct_off_low": 5.0,
        "candle_quality": 0.5,
        "tech_score": 1.0,
    }
    try:
        df = yf.download(ticker, period="1d", interval="1m",
                         progress=False, auto_adjust=True)
        if df.empty or len(df) < 6:
            return neutral

        # Flatten MultiIndex columns if present
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        # ── VWAP ──────────────────────────────────────────────────────────────
        df["typical"] = (df["High"] + df["Low"] + df["Close"]) / 3
        total_vol = df["Volume"].sum()
        vwap = float((df["typical"] * df["Volume"]).sum() / total_vol) if total_vol > 0 else None
        last_price = float(df["Close"].iloc[-1])
        above_vwap = (last_price > vwap) if vwap else None

        # ── Trend: last 5 candles ─────────────────────────────────────────────
        closes = df["Close"].tail(5).values.flatten()
        ups   = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i-1])
        downs = sum(1 for i in range(1, len(closes)) if closes[i] < closes[i-1])
        trend = "UP" if ups >= 3 else ("DOWN" if downs >= 3 else "FLAT")

        # ── Volume acceleration: last 5 min vs prior 10 min ──────────────────
        recent_vol = float(df["Volume"].tail(5).mean())
        prior_vol  = float(df["Volume"].tail(15).head(10).mean())
        vol_accel  = (recent_vol / prior_vol) if prior_vol > 0 else 1.0

        # ── Distance from day high / low ──────────────────────────────────────
        day_high = float(df["High"].max())
        day_low  = float(df["Low"].min())
        pct_off_high = (day_high - last_price) / day_high * 100 if day_high > 0 else 0
        pct_off_low  = (last_price - day_low)  / day_low  * 100 if day_low  > 0 else 0

        # ── Candle quality: avg body/range of last 5 candles ─────────────────
        # High ratio = clean directional moves; low = choppy wicks
        ratios = []
        for _, row in df.tail(5).iterrows():
            rng  = float(row["High"]) - float(row["Low"])
            body = abs(float(row["Close"]) - float(row["Open"]))
            if rng > 0:
                ratios.append(body / rng)
        candle_quality = sum(ratios) / len(ratios) if ratios else 0.5

        # ── Composite technical score (multiplier applied to base signal) ─────
        # Each factor adds or subtracts from a 1.0 base
        tech = 1.0
        if above_vwap is True:   tech += 0.30   # above VWAP = bullish
        if above_vwap is False:  tech -= 0.10   # slight penalty below VWAP
        if trend == "UP":        tech += 0.25   # recent candles trending up
        if trend == "DOWN":      tech -= 0.20   # trending against us
        if vol_accel > 1.5:      tech += 0.20   # volume accelerating
        if vol_accel < 0.7:      tech -= 0.15   # volume dying out
        if pct_off_high < 2.0:   tech += 0.15   # near day high (longs: strong)
        if pct_off_high > 8.0:   tech -= 0.15   # far from high (fading)
        if candle_quality > 0.6: tech += 0.10   # clean candles
        if candle_quality < 0.3: tech -= 0.10   # choppy wicks

        return {
            "vwap":          vwap,
            "above_vwap":    above_vwap,
            "trend":         trend,
            "vol_accel":     vol_accel,
            "pct_off_high":  pct_off_high,
            "pct_off_low":   pct_off_low,
            "candle_quality": candle_quality,
            "tech_score":    max(tech, 0.1),   # never go below 0.1×
        }
    except Exception:
        return neutral


def tech_summary(t: dict) -> str:
    """One-line string showing key technical factors."""
    vwap_str = "↑VWAP" if t["above_vwap"] else ("↓VWAP" if t["above_vwap"] is False else "VWAP?")
    return (f"{vwap_str}  trend={t['trend']}  "
            f"vaccel={t['vol_accel']:.1f}x  "
            f"offHigh={t['pct_off_high']:.1f}%  "
            f"cq={t['candle_quality']:.2f}  "
            f"→ techx{t['tech_score']:.2f}")


# ── Signal scoring ─────────────────────────────────────────────────────────────

def score_long(q: dict, intraday_highs: dict) -> tuple[float, str]:
    """Returns (base_score, signal_name). Score 0 = no signal."""
    ticker = q["ticker"]
    change = q["change"]
    rv     = q["rel_vol"]

    # Signal 1: Gap at open (big gap up with volume)
    if change >= 5.0 and rv >= 2.0:
        score = change * rv * 1.2   # boost gap signals
        return score, "GAP"

    # Signal 2: Momentum continuation (already up 10%+ and still at day high)
    if change >= 10.0 and q["price"] >= q["day_high"] * 0.995:
        score = change * rv
        return score, "MOMENTUM"

    # Signal 3: Intraday breakout (price just broke above previous day high)
    prev_high = intraday_highs.get(ticker, 0)
    if prev_high > 0 and q["price"] > prev_high * 1.01 and rv >= 1.5:
        score = change * rv * 0.9
        return score, "BREAKOUT"

    return 0.0, ""


def score_short(q: dict) -> tuple[float, str]:
    change = q["change"]
    rv     = q["rel_vol"]

    # Signal 1: Gap down with volume
    if change <= -5.0 and rv >= 2.0:
        return abs(change) * rv * 1.2, "GAP DOWN"

    # Signal 2: Momentum down — already down 10%+ at day low
    if change <= -10.0 and q["price"] <= q["day_low"] * 1.005:
        return abs(change) * rv, "MOMENTUM DOWN"

    return 0.0, ""


# ── Position sizing ────────────────────────────────────────────────────────────

def calc_position_size(score: float, open_count: int) -> int:
    """Scale size based on signal strength and available slots."""
    per_slot    = TOTAL_CAPITAL / MAX_POSITIONS
    # Boost size for strong signals (score > 50 = full size, < 20 = 60%)
    strength    = min(score / 50.0, 1.0)
    size_factor = 0.6 + 0.4 * strength
    return int(per_slot * size_factor)


# ── Trade execution ────────────────────────────────────────────────────────────

def place_order(ticker: str, side: str, shares: int):
    """side: BUY | SHORT | SELL | COVER"""
    action_label = {
        "BUY":   "Buy",
        "SHORT": "Sell Short",
        "SELL":  "Sell",
        "COVER": "Buy to Cover",
    }[side]

    run_applescript('tell application "Safari" to activate')
    open_url(f"https://www.marketwatch.com/games/{MW_GAME}/trade")

    safari_js('document.querySelector("input.j-miniTrade").focus(); document.querySelector("input.j-miniTrade").click();')
    time.sleep(0.4)
    for char in ticker:
        safari_js(f"""
            var inp = document.querySelector('input.j-miniTrade');
            inp.value += '{char}';
            inp.dispatchEvent(new KeyboardEvent('keydown', {{key:'{char}', bubbles:true}}));
            inp.dispatchEvent(new InputEvent('input', {{data:'{char}', inputType:'insertText', bubbles:true}}));
            inp.dispatchEvent(new KeyboardEvent('keyup', {{key:'{char}', bubbles:true}}));
        """)
        time.sleep(0.25)
    time.sleep(2)

    safari_js('document.querySelector("button.j-trade").click();')
    time.sleep(2.5)

    safari_js(f"""
        var btns = document.querySelectorAll('label, button');
        for (var b of btns) {{
            if (b.textContent.trim() === '{action_label}') {{
                b.click(); break;
            }}
        }}
    """)
    time.sleep(0.6)

    safari_js(f"""
        var inp = document.querySelector('input[name="shares"]');
        inp.focus(); inp.value = '{shares}';
        inp.dispatchEvent(new Event('input', {{bubbles:true}}));
        inp.dispatchEvent(new Event('change', {{bubbles:true}}));
    """)
    time.sleep(0.4)

    safari_js('document.querySelector("#priceType").value = "None"; document.querySelector("#priceType").dispatchEvent(new Event("change", {bubbles:true}));')
    time.sleep(0.3)

    safari_js("""
        var btns = document.querySelectorAll('button');
        for (var b of btns) {
            if (b.textContent.trim() === 'Submit Order') { b.click(); break; }
        }
    """)
    time.sleep(2)


# ── Position tracking ──────────────────────────────────────────────────────────

class Position:
    def __init__(self, ticker, side, shares, entry_price, signal):
        self.ticker       = ticker
        self.side         = side
        self.shares       = shares
        self.entry_price  = entry_price
        self.signal       = signal
        self.peak_pnl     = 0.0       # for trailing stop
        self.scaled_out   = False     # have we sold 50% yet
        self.closed       = False
        self.pnl_usd      = 0.0

    def pnl_pct(self, price: float) -> float:
        if self.side == "BUY":
            return (price - self.entry_price) / self.entry_price * 100
        return (self.entry_price - price) / self.entry_price * 100

    def pnl_dollars(self, price: float) -> float:
        return self.pnl_pct(price) / 100 * self.shares * self.entry_price


def close_pos(pos: Position, price: float, shares: int, reason: str) -> float:
    side    = "SELL" if pos.side == "BUY" else "COVER"
    pnl_pct = pos.pnl_pct(price)
    pnl_usd = pnl_pct / 100 * shares * pos.entry_price
    print(f"\n  [{reason}]  {pos.ticker}  {pnl_pct:+.1f}%  ${pnl_usd:+,.0f}  → {side} {shares:,}")
    place_order(pos.ticker, side, shares)
    return pnl_usd


# ── Scanning ───────────────────────────────────────────────────────────────────

def scan(positions: list[Position], intraday_highs: dict) -> list[dict]:
    held    = {p.ticker for p in positions if not p.closed}
    recent  = {p.ticker for p in positions[-6:]}
    exclude = held | recent

    universe         = get_universe()
    long_candidates  = []
    short_candidates = []

    # ── Pass 1: quick filter (price, volume, gap %) ───────────────────────────
    for ticker in universe:
        if ticker in exclude:
            continue
        q = get_quote(ticker)
        if not q or not passes_filters(q):
            continue

        ls, lsig = score_long(q, intraday_highs)
        if ls > 0:
            long_candidates.append((ls, lsig, q))

        ss, ssig = score_short(q)
        if ss > 0:
            short_candidates.append((ss, ssig, q))

    long_candidates.sort(reverse=True)
    short_candidates.sort(reverse=True)

    # ── Pass 2: deep technical analysis on top 5 per side ────────────────────
    # This is slower (fetches 1-min candles) but only runs on best candidates.
    results = []

    for base_score, signal, q in long_candidates[:5]:
        ticker = q["ticker"]
        print(f"    Analyzing {ticker} [{signal}] base_score={base_score:.1f} …")
        t = analyze_technicals(ticker)
        print(f"      {tech_summary(t)}")

        # Block longs that are trending down or far below VWAP
        if t["trend"] == "DOWN" and t["above_vwap"] is False:
            print(f"      ✗ {ticker} skipped — trending DOWN below VWAP")
            continue

        final_score = base_score * t["tech_score"]
        results.append({
            "side":   "BUY",
            "score":  final_score,
            "signal": signal,
            "q":      q,
            "tech":   t,
        })

    for base_score, signal, q in short_candidates[:5]:
        ticker = q["ticker"]
        print(f"    Analyzing {ticker} [{signal}] base_score={base_score:.1f} …")
        t = analyze_technicals(ticker)
        # For shorts, invert the VWAP/trend logic
        short_tech = t.copy()
        tech = 1.0
        if t["above_vwap"] is False: tech += 0.30   # below VWAP = bearish (good for short)
        if t["above_vwap"] is True:  tech -= 0.10
        if t["trend"] == "DOWN":     tech += 0.25
        if t["trend"] == "UP":       tech -= 0.20
        if t["vol_accel"] > 1.5:     tech += 0.20
        if t["vol_accel"] < 0.7:     tech -= 0.15
        if t["pct_off_low"] < 2.0:   tech += 0.15   # near day low = strong short
        if t["pct_off_low"] > 8.0:   tech -= 0.15
        if t["candle_quality"] > 0.6: tech += 0.10
        if t["candle_quality"] < 0.3: tech -= 0.10
        short_tech["tech_score"] = max(tech, 0.1)
        print(f"      {tech_summary(short_tech)}")

        # Block shorts that are trending up and above VWAP
        if t["trend"] == "UP" and t["above_vwap"] is True:
            print(f"      ✗ {ticker} skipped — trending UP above VWAP")
            continue

        final_score = base_score * short_tech["tech_score"]
        results.append({
            "side":   "SHORT",
            "score":  final_score,
            "signal": signal,
            "q":      q,
            "tech":   short_tech,
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


def scan_fade(positions: list[Position]) -> dict | None:
    held    = {p.ticker for p in positions if not p.closed}
    universe = get_universe()
    best = None
    for ticker in universe:
        if ticker in held:
            continue
        q = get_quote(ticker)
        if not q or q["price"] < MIN_PRICE:
            continue
        if q["change"] >= FADE_MIN_PCT:
            if best is None or q["change"] > best["change"]:
                best = q
    return best


# ── Main loop ──────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 65)
    print(f"  MarketWatch Advanced Day Trader  —  {datetime.now().strftime('%b %d %Y  %H:%M')}")
    print(f"  Capital: ${TOTAL_CAPITAL:,.0f}  |  Max positions: {MAX_POSITIONS}")
    print(f"  Scale out at +{SCALE_OUT_PCT:.0f}%  |  Full target +{FULL_TARGET_PCT:.0f}%  |  Stop {STOP_PCT:.0f}%  |  Trail {TRAIL_PCT:.0f}%")
    print("=" * 65 + "\n")

    positions      : list[Position] = []
    intraday_highs : dict           = {}   # ticker → previous scan's day_high
    fade_done      = False
    total_pnl      = 0.0
    trade_count    = 0

    while True:
        now        = datetime.now()
        hour, mins = now.hour, now.minute

        if hour < 9 or (hour == 9 and mins < 30):
            print(f"  Waiting for market open …")
            time.sleep(30)
            continue
        if hour >= 16:
            print(f"\n  Market closed.  P&L: ${total_pnl:+,.0f}  |  Trades: {trade_count}\n")
            break

        open_positions = [p for p in positions if not p.closed]

        # ── Force close at 3:55 ────────────────────────────────────────────────
        if (hour, mins) >= CLOSE_TIME:
            for pos in open_positions:
                q = get_quote(pos.ticker)
                price = q["price"] if q else pos.entry_price
                total_shares = pos.shares if pos.scaled_out else pos.shares
                pnl = close_pos(pos, price, total_shares, "EOD")
                total_pnl += pnl
                pos.closed = True
            print(f"\n  3:55 PM — all closed.  P&L: ${total_pnl:+,.0f}  |  Trades: {trade_count}\n")
            break

        # ── Fade scan at 3:45 ──────────────────────────────────────────────────
        if (hour, mins) >= FADE_TIME and not fade_done:
            fade_done = True
            if len(open_positions) < MAX_POSITIONS:
                print(f"\n  [{now.strftime('%H:%M')}] Fade scan …")
                fq = scan_fade(positions)
                if fq:
                    shares = int(TOTAL_CAPITAL / MAX_POSITIONS / fq["price"])
                    print(f"  FADE SHORT  {fq['ticker']}  up {fq['change']:+.1f}%  →  {shares:,} @ ${fq['price']:.2f}")
                    place_order(fq["ticker"], "SHORT", shares)
                    pos = Position(fq["ticker"], "SHORT", shares, fq["price"], "FADE")
                    positions.append(pos)
                    open_positions.append(pos)
                    trade_count += 1

        # ── Monitor open positions ─────────────────────────────────────────────
        for pos in open_positions:
            q = get_quote(pos.ticker)
            if not q:
                continue
            price   = q["price"]
            pnl_pct = pos.pnl_pct(price)
            pnl_usd = pos.pnl_dollars(price)

            # Update trailing stop peak
            if pnl_pct > pos.peak_pnl:
                pos.peak_pnl = pnl_pct

            trail_trigger = pos.peak_pnl - TRAIL_PCT

            print(f"  [{now.strftime('%H:%M:%S')}]  {pos.side:5}  {pos.ticker:<6}  "
                  f"${pos.entry_price:.2f}→${price:.2f}  "
                  f"{pnl_pct:+.1f}%  ${pnl_usd:+,.0f}  "
                  f"[{pos.signal}]  peak={pos.peak_pnl:+.1f}%")

            # Hard stop
            if pnl_pct <= STOP_PCT:
                pnl = close_pos(pos, price, pos.shares, f"STOP {pnl_pct:.1f}%")
                total_pnl += pnl
                pos.closed = True
                trade_count += 1
                continue

            # Trailing stop (only after we're in profit)
            if pos.peak_pnl >= SCALE_OUT_PCT and pnl_pct <= trail_trigger:
                remaining = pos.shares // 2 if pos.scaled_out else pos.shares
                pnl = close_pos(pos, price, remaining, f"TRAIL {pnl_pct:.1f}% (peak {pos.peak_pnl:.1f}%)")
                total_pnl += pnl
                pos.closed = True
                trade_count += 1
                continue

            # Scale out at +3% (sell half)
            if not pos.scaled_out and pnl_pct >= SCALE_OUT_PCT:
                half = pos.shares // 2
                pnl = close_pos(pos, price, half, f"SCALE OUT +{pnl_pct:.1f}%")
                total_pnl += pnl
                pos.shares    -= half
                pos.scaled_out = True
                trade_count   += 1

            # Full target at +7%
            if pnl_pct >= FULL_TARGET_PCT:
                pnl = close_pos(pos, price, pos.shares, f"TARGET +{pnl_pct:.1f}%")
                total_pnl += pnl
                pos.closed = True
                trade_count += 1

        # ── Find new trades if slots available ─────────────────────────────────
        open_positions = [p for p in positions if not p.closed]
        slots = MAX_POSITIONS - len(open_positions)

        if slots > 0 and (hour, mins) < FADE_TIME:
            candidates = scan(positions, intraday_highs)
            for c in candidates[:slots]:
                q      = c["q"]
                side   = c["side"]
                signal = c["signal"]
                score  = c["score"]
                shares = calc_position_size(score, len(open_positions))
                shares = int(min(shares, TOTAL_CAPITAL * 0.95) / q["price"])
                if shares <= 0:
                    continue
                t = c.get("tech", {})
                print(f"\n  NEW  {side:5}  {q['ticker']}  [{signal}]  "
                      f"{q['change']:+.1f}%  rv={q['rel_vol']:.1f}x  score={score:.0f}  "
                      f"→  {shares:,} @ ${q['price']:.2f}")
                if t:
                    print(f"       {tech_summary(t)}")
                place_order(q["ticker"], side, shares)
                pos = Position(q["ticker"], side, shares, q["price"], signal)
                positions.append(pos)
                open_positions.append(pos)
                trade_count += 1

        # Update intraday highs for breakout detection next scan
        for p in open_positions:
            q = get_quote(p.ticker)
            if q:
                intraday_highs[p.ticker] = q["day_high"]

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
