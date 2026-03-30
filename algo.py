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


# ── Signal scoring ─────────────────────────────────────────────────────────────

def score_long(q: dict, intraday_highs: dict) -> tuple[float, str]:
    """Returns (score, signal_name). Score 0 = no signal."""
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
    recent  = {p.ticker for p in positions[-6:]}   # avoid re-entering recent trades
    exclude = held | recent

    universe        = get_universe()
    long_candidates  = []
    short_candidates = []

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

    results = []
    for score, signal, q in long_candidates[:1]:
        results.append({"side": "BUY",   "score": score, "signal": signal, "q": q})
    for score, signal, q in short_candidates[:1]:
        results.append({"side": "SHORT", "score": score, "signal": signal, "q": q})

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
                print(f"\n  NEW  {side:5}  {q['ticker']}  [{signal}]  "
                      f"{q['change']:+.1f}%  rv={q['rel_vol']:.1f}x  score={score:.0f}  "
                      f"→  {shares:,} @ ${q['price']:.2f}")
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
