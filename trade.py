"""
MarketWatch VSE  —  Auto Trader (Safari via AppleScript)
=========================================================
Controls your actual Safari browser to place trades.
No login needed — uses your existing Safari session.

Run:
    python3 trade.py

Best run at 9:25 AM so the order is ready right at open.
"""

import os
import sys
import time
import subprocess
import json
from datetime import datetime

MW_GAME = "yuse-spring-2026-stock-market-competition-"


# ── AppleScript helpers ───────────────────────────────────────────────────────

def run_applescript(script: str) -> str:
    result = subprocess.run(
        ["osascript", "-e", script],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"AppleScript error: {result.stderr.strip()}")
    return result.stdout.strip()


def safari_js(js: str) -> str:
    """Run JavaScript in the frontmost Safari tab and return the result."""
    escaped = js.replace("\\", "\\\\").replace('"', '\\"')
    script = f'''
        tell application "Safari"
            tell front document
                do JavaScript "{escaped}"
            end tell
        end tell
    '''
    return run_applescript(script)


def open_url(url: str):
    """Open a URL in Safari."""
    script = f'''
        tell application "Safari"
            activate
            open location "{url}"
            delay 3
        end tell
    '''
    run_applescript(script)


def wait_for_page(keyword: str, timeout: int = 15):
    """Wait until the Safari page URL or title contains keyword."""
    for _ in range(timeout):
        try:
            info = run_applescript('''
                tell application "Safari"
                    return (URL of front document) & " " & (name of front document)
                end tell
            ''')
            if keyword.lower() in info.lower():
                return
        except Exception:
            pass
        time.sleep(1)


# ── Get today's recommendations ───────────────────────────────────────────────

def get_recommendations():
    sys.path.insert(0, os.path.dirname(__file__))
    import backtest as b
    import yfinance as yf

    live = b.fetch_live_movers(b.BT)
    if live:
        b.BT["gap_universe"] = list(dict.fromkeys(b.BT["gap_universe"] + live))

    cfg     = b.BT
    universe = cfg["gap_universe"]

    long_candidates  = []
    short_candidates = []

    print(f"Scanning {len(universe)} tickers for today's setup …")
    for ticker in universe:
        try:
            fi         = yf.Ticker(ticker).fast_info
            prev_close = getattr(fi, "previous_close", None)
            current    = getattr(fi, "last_price",     None)
            volume     = getattr(fi, "three_month_average_volume", None)
            last_vol   = getattr(fi, "last_volume",    None)

            if not prev_close or not current or prev_close <= 0:
                continue
            if current < cfg.get("live_movers_min_price", 2.0):
                continue

            gap     = (current - prev_close) / prev_close * 100
            rel_vol = (last_vol / volume) if volume and last_vol else 0.0
            score   = abs(gap) * max(rel_vol, 0.1)

            if cfg["gap_min_pct"] <= gap <= cfg["gap_max_pct"]:
                long_candidates.append((score, gap, ticker, current, rel_vol))
            elif -cfg["gap_short_max_pct"] <= gap <= -cfg["gap_short_min_pct"]:
                short_candidates.append((score, gap, ticker, current, rel_vol))
        except Exception:
            pass

    long_candidates.sort(reverse=True)
    short_candidates.sort(reverse=True)

    trades = []
    if long_candidates:
        score, gap, ticker, price, rv = long_candidates[0]
        shares = int(cfg["gap_order_usd"] / price)
        trades.append({"side": "BUY", "ticker": ticker, "shares": shares,
                       "price": price, "gap": gap, "rv": rv})
    if short_candidates:
        score, gap, ticker, price, rv = short_candidates[0]
        shares = int(cfg["gap_order_usd"] / price)
        trades.append({"side": "SHORT", "ticker": ticker, "shares": shares,
                       "price": price, "gap": gap, "rv": rv})

    return trades, cfg


# ── Place a trade via Safari ──────────────────────────────────────────────────

def place_trade(trade: dict):
    side   = trade["side"]
    ticker = trade["ticker"]
    shares = trade["shares"]
    price  = trade["price"]
    gap    = trade["gap"]

    print(f"\n  Placing {side} {shares:,} x {ticker}  "
          f"(gap {gap:+.1f}%  ~${shares * price:,.0f}) …")

    url = f"https://www.marketwatch.com/games/{MW_GAME}/trade"
    open_url(url)
    time.sleep(3)

    # ── Type ticker character by character to trigger autocomplete ────────────
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

    # ── Click the Trade button next to the ticker result ─────────────────────
    safari_js('document.querySelector("button.j-trade").click();')
    time.sleep(2)

    # ── Select Buy or Short ───────────────────────────────────────────────────
    order_id = "order-buy" if side == "BUY" else "order-short"
    safari_js(f'document.querySelector("#{order_id}").click();')
    time.sleep(0.4)

    # ── Enter number of shares ────────────────────────────────────────────────
    safari_js(f"""
        var inp = document.querySelector('input[name="shares"]');
        inp.focus();
        inp.value = '{shares}';
        inp.dispatchEvent(new Event('input', {{bubbles:true}}));
        inp.dispatchEvent(new Event('change', {{bubbles:true}}));
    """)
    time.sleep(0.4)

    # ── Select Market order ───────────────────────────────────────────────────
    safari_js('document.querySelector("#priceType").value = "None"; document.querySelector("#priceType").dispatchEvent(new Event("change", {bubbles:true}));')
    time.sleep(0.4)

    # ── Submit order ──────────────────────────────────────────────────────────
    safari_js('document.querySelector("button.j-submit").click();')
    time.sleep(2)

    print(f"  Done: {side} {shares:,} x {ticker}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 60)
    print(f"  MarketWatch Auto Trader  —  {datetime.now().strftime('%b %d %Y  %H:%M')}")
    print("=" * 60 + "\n")

    trades, cfg = get_recommendations()

    if not trades:
        print("No qualifying setups today. Nothing to trade.\n")
        return

    print(f"\nFound {len(trades)} trade(s):\n")
    for t in trades:
        print(f"  {t['side']:5}  {t['ticker']}  {t['gap']:+.1f}%  "
              f"{t['shares']:,} shares  ~${t['shares'] * t['price']:,.0f}")

    print("\nOpening Safari …")

    # Make sure Safari is open and logged in
    run_applescript('tell application "Safari" to activate')
    time.sleep(1)

    for trade in trades:
        place_trade(trade)

    # Save positions so monitor.py knows what we're holding
    positions = [{
        "ticker":      t["ticker"],
        "side":        t["side"],
        "shares":      t["shares"],
        "entry_price": t["price"],
        "date":        datetime.now().strftime("%Y-%m-%d"),
    } for t in trades]
    positions_file = os.path.join(os.path.dirname(__file__), "positions.json")
    with open(positions_file, "w") as f:
        json.dump(positions, f, indent=2)
    print(f"Positions saved to positions.json\n")

    print("\nAll trades placed. Now run:  python3 monitor.py\n")


if __name__ == "__main__":
    main()
