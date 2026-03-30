"""
MarketWatch VSE  —  Position Monitor
======================================
Run this after trade.py each morning. It:
  • Checks your positions every 2 minutes
  • Sells/covers at +15% target or -6% stop
  • At 3:45 PM: shorts stocks up 100%+ (fade strategy)
  • At 3:55 PM: force-closes everything still open

Run:
    python3 monitor.py
"""

import json
import os
import sys
import time
import subprocess
import random
from datetime import datetime, date

import yfinance as yf

# ── Config ─────────────────────────────────────────────────────────────────────

POSITIONS_FILE  = os.path.join(os.path.dirname(__file__), "positions.json")
MW_GAME         = "yuse-spring-2026-stock-market-competition-"
CHECK_INTERVAL  = 120          # seconds between price checks
TARGET_PCT      =  15.0        # take profit
STOP_PCT        =  -6.0        # cut loss
FADE_MIN_PCT    = 100.0        # short stocks up this much % on the day
FADE_ORDER_USD  = 200_000      # position size for fade trades
FADE_TIME       = (15, 45)     # run fade scanner at 3:45 PM
CLOSE_TIME      = (15, 55)     # force-close all at 3:55 PM


# ── AppleScript / Safari helpers ───────────────────────────────────────────────

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


# ── Price fetching ─────────────────────────────────────────────────────────────

def get_price(ticker: str) -> float | None:
    try:
        fi = yf.Ticker(ticker).fast_info
        p  = getattr(fi, "last_price", None)
        return float(p) if p else None
    except Exception:
        return None


def get_change_pct(ticker: str) -> float | None:
    """Today's % change from previous close."""
    try:
        fi         = yf.Ticker(ticker).fast_info
        prev       = getattr(fi, "previous_close", None)
        current    = getattr(fi, "last_price",     None)
        if prev and current and prev > 0:
            return (current - prev) / prev * 100
    except Exception:
        pass
    return None


# ── Trade execution ────────────────────────────────────────────────────────────

def place_order(ticker: str, side: str, shares: int):
    """side: BUY | SHORT | SELL | COVER"""
    print(f"  >>> {side} {shares:,} x {ticker}")
    run_applescript('tell application "Safari" to activate')
    open_url(f"https://www.marketwatch.com/games/{MW_GAME}/trade")

    # Type ticker character by character
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

    # Click Trade button
    safari_js('document.querySelector("button.j-trade").click();')
    time.sleep(2)

    # Select order type
    order_id = {"BUY": "order-buy", "SHORT": "order-short",
                "SELL": "order-sell", "COVER": "order-cover"}[side]
    safari_js(f'document.querySelector("#{order_id}").click();')
    time.sleep(0.4)

    # Enter shares
    safari_js(f"""
        var inp = document.querySelector('input[name="shares"]');
        inp.focus(); inp.value = '{shares}';
        inp.dispatchEvent(new Event('input', {{bubbles:true}}));
        inp.dispatchEvent(new Event('change', {{bubbles:true}}));
    """)
    time.sleep(0.4)

    # Market order & submit
    safari_js('document.querySelector("#priceType").value = "None"; document.querySelector("#priceType").dispatchEvent(new Event("change", {bubbles:true}));')
    time.sleep(0.3)
    safari_js('document.querySelector("button.j-submit").click();')
    time.sleep(2)


def close_position(pos: dict, reason: str):
    side   = "SELL" if pos["side"] == "BUY" else "COVER"
    ticker = pos["ticker"]
    shares = pos["shares"]
    price  = get_price(ticker) or pos["entry_price"]
    if pos["side"] == "BUY":
        pnl_pct = (price - pos["entry_price"]) / pos["entry_price"] * 100
    else:
        pnl_pct = (pos["entry_price"] - price) / pos["entry_price"] * 100
    print(f"\n  [{reason}] Closing {ticker}  {pnl_pct:+.1f}%  →  {side} {shares:,} shares")
    place_order(ticker, side, shares)
    pos["closed"] = True


# ── Fade scanner (pre-close) ───────────────────────────────────────────────────

def run_fade_scanner(open_tickers: set[str]):
    """Short stocks up 100%+ — they tend to fade at next open."""
    sys.path.insert(0, os.path.dirname(__file__))
    import backtest as b

    live = b.fetch_live_movers(b.BT)
    universe = list(dict.fromkeys(b.BT["gap_universe"] + live))

    candidates = []
    print("\n  Scanning for pre-close fade setups …")
    for ticker in universe:
        if ticker in open_tickers:
            continue
        chg = get_change_pct(ticker)
        if chg is None:
            continue
        price = get_price(ticker)
        if not price or price < 1.0:
            continue
        if chg >= FADE_MIN_PCT:
            candidates.append((chg, ticker, price))

    candidates.sort(reverse=True)

    if not candidates:
        print("  No fade setups found today.")
        return []

    new_positions = []
    for chg, ticker, price in candidates[:1]:   # top 1 fade
        shares = int(FADE_ORDER_USD / price)
        print(f"\n  FADE SHORT  {ticker}  up {chg:+.1f}%  →  SHORT {shares:,} @ ${price:.2f}")
        place_order(ticker, "SHORT", shares)
        new_positions.append({
            "ticker":      ticker,
            "side":        "SHORT",
            "shares":      shares,
            "entry_price": price,
            "date":        date.today().isoformat(),
            "fade":        True,
        })

    return new_positions


# ── Main monitor loop ──────────────────────────────────────────────────────────

def main():
    if not os.path.exists(POSITIONS_FILE):
        print("No positions.json found. Run trade.py first.\n")
        sys.exit(1)

    with open(POSITIONS_FILE) as f:
        positions = json.load(f)

    # Filter to today's positions only
    today = date.today().isoformat()
    positions = [p for p in positions if p.get("date") == today and not p.get("closed")]

    if not positions:
        print("No open positions for today.\n")

    print("\n" + "=" * 60)
    print(f"  Position Monitor  —  {datetime.now().strftime('%b %d %Y  %H:%M')}")
    print("=" * 60)
    print(f"  Monitoring {len(positions)} position(s)  |  "
          f"Target: +{TARGET_PCT:.0f}%  Stop: {STOP_PCT:.0f}%")
    print(f"  Fade scan at 3:45 PM  |  Force-close at 3:55 PM\n")

    fade_done       = False
    force_close_done = False

    while True:
        now  = datetime.now()
        hour = now.hour
        mins = now.minute

        # ── Outside market hours ───────────────────────────────────────────────
        if hour < 9 or (hour == 9 and mins < 30) or hour >= 16:
            print(f"  [{now.strftime('%H:%M')}] Market closed. Exiting.")
            break

        open_positions = [p for p in positions if not p.get("closed")]

        # ── Force-close at 3:55 PM ─────────────────────────────────────────────
        if (hour, mins) >= CLOSE_TIME and not force_close_done:
            print(f"\n  [{now.strftime('%H:%M')}] 3:55 PM — force-closing all positions …")
            for pos in open_positions:
                close_position(pos, "EOD")
            force_close_done = True
            break

        # ── Fade scanner at 3:45 PM ────────────────────────────────────────────
        if (hour, mins) >= FADE_TIME and not fade_done:
            held = {p["ticker"] for p in positions if not p.get("closed")}
            new  = run_fade_scanner(held)
            positions.extend(new)
            fade_done = True

        # ── Check stop/target for each open position ───────────────────────────
        print(f"\n  [{now.strftime('%H:%M')}] Checking {len(open_positions)} position(s) …")
        for pos in open_positions:
            price = get_price(pos["ticker"])
            if price is None:
                print(f"    {pos['ticker']}: no price data")
                continue

            entry = pos["entry_price"]
            if pos["side"] == "BUY":
                pnl_pct = (price - entry) / entry * 100
            else:
                pnl_pct = (entry - price) / entry * 100

            pnl_usd = pnl_pct / 100 * pos["shares"] * entry
            print(f"    {pos['side']:5}  {pos['ticker']:<6}  "
                  f"entry ${entry:.2f}  now ${price:.2f}  "
                  f"{pnl_pct:+.1f}%  ${pnl_usd:+,.0f}")

            if pnl_pct >= TARGET_PCT:
                close_position(pos, f"TARGET +{pnl_pct:.1f}%")
            elif pnl_pct <= STOP_PCT:
                close_position(pos, f"STOP {pnl_pct:.1f}%")

        # ── Save updated positions ─────────────────────────────────────────────
        with open(POSITIONS_FILE, "w") as f:
            json.dump(positions, f, indent=2)

        if not [p for p in positions if not p.get("closed")]:
            print("\n  All positions closed. Done.\n")
            break

        print(f"  Next check in {CHECK_INTERVAL // 60} min …")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
