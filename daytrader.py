"""
MarketWatch VSE  —  All-Day Trader
====================================
Runs continuously from 9:30 AM to 3:55 PM.
  • Scans for the best momentum trade right now
  • Enters, monitors every 30 seconds
  • Exits at +5% target or -3% stop
  • Immediately finds the next trade
  • 3:45 PM: shorts stocks up 100%+ (fade play)
  • 3:55 PM: force-closes everything

Run:
    python3 daytrader.py
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

MW_GAME        = "yuse-spring-2026-stock-market-competition-"
TARGET_PCT     =  5.0    # take profit
STOP_PCT       = -3.0    # stop loss
CHECK_INTERVAL =  30     # seconds between price checks
MIN_GAP_PCT    =  3.0    # minimum move % to enter a trade
MIN_RELVOL     =  1.5    # minimum relative volume
MIN_PRICE      =  2.0    # minimum stock price
ORDER_USD      = 200_000 # position size
FADE_MIN_PCT   = 100.0   # short stocks up this % near close
FADE_TIME      = (15, 45)
CLOSE_TIME     = (15, 55)


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
        fi          = yf.Ticker(ticker).fast_info
        prev        = getattr(fi, "previous_close",            None)
        price       = getattr(fi, "last_price",                None)
        last_vol    = getattr(fi, "last_volume",               None)
        avg_vol     = getattr(fi, "three_month_average_volume", None)
        if not prev or not price or prev <= 0:
            return None
        change  = (price - prev) / prev * 100
        rel_vol = (last_vol / avg_vol) if avg_vol and last_vol else 0.0
        return {"ticker": ticker, "price": price, "change": change, "rel_vol": rel_vol}
    except Exception:
        return None


def get_universe() -> list[str]:
    sys.path.insert(0, os.path.dirname(__file__))
    import backtest as b
    live = b.fetch_live_movers(b.BT)
    return list(dict.fromkeys(b.BT["gap_universe"] + live))


# ── Trade execution ────────────────────────────────────────────────────────────

def place_order(ticker: str, side: str, shares: int):
    """side: BUY | SHORT | SELL | COVER"""
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
    time.sleep(2)

    order_id = {"BUY": "order-buy", "SHORT": "order-short",
                "SELL": "order-sell", "COVER": "order-cover"}[side]
    safari_js(f'document.querySelector("#{order_id}").click();')
    time.sleep(0.4)

    safari_js(f"""
        var inp = document.querySelector('input[name="shares"]');
        inp.focus(); inp.value = '{shares}';
        inp.dispatchEvent(new Event('input', {{bubbles:true}}));
        inp.dispatchEvent(new Event('change', {{bubbles:true}}));
    """)
    time.sleep(0.4)

    safari_js('document.querySelector("#priceType").value = "None"; document.querySelector("#priceType").dispatchEvent(new Event("change", {bubbles:true}));')
    time.sleep(0.3)
    safari_js('document.querySelector("button.j-submit").click();')
    time.sleep(2)


# ── Scanning ───────────────────────────────────────────────────────────────────

def scan_for_trade(exclude: set[str]) -> dict | None:
    """Find the best momentum trade available right now."""
    universe = get_universe()
    long_candidates  = []
    short_candidates = []

    for ticker in universe:
        if ticker in exclude:
            continue
        q = get_quote(ticker)
        if not q or q["price"] < MIN_PRICE:
            continue
        if q["rel_vol"] < MIN_RELVOL:
            continue
        score = abs(q["change"]) * q["rel_vol"]
        if q["change"] >= MIN_GAP_PCT:
            long_candidates.append((score, q))
        elif q["change"] <= -MIN_GAP_PCT:
            short_candidates.append((score, q))

    long_candidates.sort(reverse=True)
    short_candidates.sort(reverse=True)

    # Pick the single highest score across both sides
    best_long  = long_candidates[0]  if long_candidates  else None
    best_short = short_candidates[0] if short_candidates else None

    if best_long and best_short:
        pick = best_long if best_long[0] >= best_short[0] else best_short
    elif best_long:
        pick = best_long
    elif best_short:
        pick = best_short
    else:
        return None

    score, q  = pick
    side      = "BUY" if q["change"] > 0 else "SHORT"
    shares    = int(ORDER_USD / q["price"])
    return {
        "ticker":      q["ticker"],
        "side":        side,
        "shares":      shares,
        "entry_price": q["price"],
        "change":      q["change"],
        "rel_vol":     q["rel_vol"],
        "score":       score,
        "closed":      False,
    }


def scan_fade(exclude: set[str]) -> dict | None:
    """Find stocks up 100%+ to short before close."""
    universe = get_universe()
    candidates = []
    for ticker in universe:
        if ticker in exclude:
            continue
        q = get_quote(ticker)
        if not q or q["price"] < MIN_PRICE:
            continue
        if q["change"] >= FADE_MIN_PCT:
            candidates.append((q["change"], q))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    _, q   = candidates[0]
    shares = int(ORDER_USD / q["price"])
    return {
        "ticker":      q["ticker"],
        "side":        "SHORT",
        "shares":      shares,
        "entry_price": q["price"],
        "change":      q["change"],
        "rel_vol":     q["rel_vol"],
        "score":       q["change"],
        "closed":      False,
        "fade":        True,
    }


# ── Position management ────────────────────────────────────────────────────────

def check_position(pos: dict) -> tuple[float, float]:
    """Returns (current_price, pnl_pct)."""
    q = get_quote(pos["ticker"])
    price = q["price"] if q else pos["entry_price"]
    if pos["side"] == "BUY":
        pnl_pct = (price - pos["entry_price"]) / pos["entry_price"] * 100
    else:
        pnl_pct = (pos["entry_price"] - price) / pos["entry_price"] * 100
    return price, pnl_pct


def close_position(pos: dict, reason: str):
    side   = "SELL" if pos["side"] == "BUY" else "COVER"
    price, pnl_pct = check_position(pos)
    pnl_usd = pnl_pct / 100 * pos["shares"] * pos["entry_price"]
    print(f"\n  [{reason}] {pos['ticker']}  {pnl_pct:+.1f}%  ${pnl_usd:+,.0f}")
    print(f"  Placing {side} {pos['shares']:,} x {pos['ticker']} …")
    place_order(pos["ticker"], side, pos["shares"])
    pos["closed"] = True
    return pnl_usd


# ── Main loop ──────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 60)
    print(f"  MarketWatch All-Day Trader  —  {datetime.now().strftime('%b %d %Y  %H:%M')}")
    print(f"  Target: +{TARGET_PCT:.0f}%  |  Stop: {STOP_PCT:.0f}%  |  Size: ${ORDER_USD:,.0f}")
    print("=" * 60 + "\n")

    positions    = []      # all positions taken today (including closed)
    current_pos  = None   # currently open position
    fade_done    = False
    total_pnl    = 0.0
    trade_count  = 0

    while True:
        now  = datetime.now()
        hour, mins = now.hour, now.minute

        # Outside market hours
        if hour < 9 or (hour == 9 and mins < 30):
            print(f"  Market opens at 9:30. Waiting …")
            time.sleep(30)
            continue
        if hour >= 16:
            print(f"\n  Market closed. Total P&L today: ${total_pnl:+,.0f} ({trade_count} trades)\n")
            break

        # Force-close at 3:55
        if (hour, mins) >= CLOSE_TIME:
            if current_pos and not current_pos["closed"]:
                pnl = close_position(current_pos, "EOD")
                total_pnl += pnl
            print(f"\n  3:55 PM — done for the day.")
            print(f"  Total P&L: ${total_pnl:+,.0f}  |  Trades: {trade_count}\n")
            break

        # Fade scan at 3:45
        if (hour, mins) >= FADE_TIME and not fade_done:
            fade_done = True
            if current_pos is None or current_pos["closed"]:
                held = {p["ticker"] for p in positions if not p["closed"]}
                print(f"\n  [{now.strftime('%H:%M')}] Running fade scanner …")
                fade = scan_fade(held)
                if fade:
                    print(f"  FADE SHORT  {fade['ticker']}  up {fade['change']:+.1f}%  →  SHORT {fade['shares']:,} @ ${fade['entry_price']:.2f}")
                    place_order(fade["ticker"], "SHORT", fade["shares"])
                    positions.append(fade)
                    current_pos = fade
                    trade_count += 1
                else:
                    print("  No fade setups found.")

        # ── Monitor open position ──────────────────────────────────────────────
        if current_pos and not current_pos["closed"]:
            price, pnl_pct = check_position(current_pos)
            pnl_usd = pnl_pct / 100 * current_pos["shares"] * current_pos["entry_price"]
            print(f"  [{now.strftime('%H:%M:%S')}]  {current_pos['side']:5}  {current_pos['ticker']:<6}  "
                  f"${current_pos['entry_price']:.2f} → ${price:.2f}  "
                  f"{pnl_pct:+.1f}%  ${pnl_usd:+,.0f}")

            if pnl_pct >= TARGET_PCT:
                pnl = close_position(current_pos, f"TARGET +{pnl_pct:.1f}%")
                total_pnl += pnl
                current_pos = None
            elif pnl_pct <= STOP_PCT:
                pnl = close_position(current_pos, f"STOP {pnl_pct:.1f}%")
                total_pnl += pnl
                current_pos = None

        # ── Find next trade if no position open ────────────────────────────────
        if (current_pos is None or current_pos["closed"]) and (hour, mins) < FADE_TIME:
            held = {p["ticker"] for p in positions if not p["closed"]}
            # Also exclude recently traded tickers (avoid re-entering same loser)
            recently_traded = {p["ticker"] for p in positions[-3:]}
            exclude = held | recently_traded

            print(f"  [{now.strftime('%H:%M:%S')}]  Scanning for next trade …")
            trade = scan_for_trade(exclude)
            if trade:
                print(f"\n  NEW TRADE  {trade['side']:5}  {trade['ticker']}  "
                      f"{trade['change']:+.1f}%  rv={trade['rel_vol']:.1f}x  "
                      f"→  {trade['shares']:,} shares @ ${trade['entry_price']:.2f}")
                place_order(trade["ticker"], trade["side"], trade["shares"])
                positions.append(trade)
                current_pos = trade
                trade_count += 1
            else:
                print(f"  [{now.strftime('%H:%M:%S')}]  No setup right now. Waiting …")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
