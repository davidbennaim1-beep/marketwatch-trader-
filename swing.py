"""
MarketWatch VSE  —  Swing Trade Scanner
=========================================
Finds the best stocks to hold for 3-5 days.
Scores each stock on:
  • Trend  (price above MA20 and MA50)
  • RSI    (50-70 sweet spot — momentum without being overbought)
  • Volume (recent accumulation vs average)
  • Proximity to 52-week high (breakout territory)
  • Medium-term momentum (last 20 days)
  • Consolidation (tight price action = coiling for a move)
  • Short interest (high short % = squeeze potential)

Run:
    python3 swing.py
"""

import os
import sys
import pandas as pd
import yfinance as yf
from datetime import datetime

# ── Config ─────────────────────────────────────────────────────────────────────

MIN_PRICE   = 3.0
MIN_AVG_VOL = 300_000    # minimum average daily volume
TOP_N       = 5          # how many picks to show
ORDER_USD   = 400_000    # MarketWatch position size


# ── Universe ───────────────────────────────────────────────────────────────────

def get_universe() -> list[str]:
    sys.path.insert(0, os.path.dirname(__file__))
    import backtest as b
    live = b.fetch_live_movers(b.BT)
    return list(dict.fromkeys(b.BT["gap_universe"] + live))


# ── Indicators ─────────────────────────────────────────────────────────────────

def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain  = delta.clip(lower=0).rolling(period).mean()
    loss  = (-delta.clip(upper=0)).rolling(period).mean()
    rs    = gain / loss
    return 100 - (100 / (1 + rs))


# ── Per-ticker analysis ────────────────────────────────────────────────────────

def analyze(ticker: str) -> dict | None:
    try:
        df = yf.download(ticker, period="6mo", interval="1d",
                         progress=False, auto_adjust=True)
        if df.empty or len(df) < 52:
            return None

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        close  = df["Close"].squeeze()
        volume = df["Volume"].squeeze()
        high   = df["High"].squeeze()
        low    = df["Low"].squeeze()

        price   = float(close.iloc[-1])
        avg_vol = float(volume.tail(20).mean())

        if price < MIN_PRICE or avg_vol < MIN_AVG_VOL:
            return None

        # ── Indicators ────────────────────────────────────────────────────────
        ma20 = float(close.rolling(20).mean().iloc[-1])
        ma50 = float(close.rolling(50).mean().iloc[-1])
        rsi  = float(calc_rsi(close).iloc[-1])

        # Volume: last 3 days vs 20-day average
        vol_ratio = float(volume.tail(3).mean() / avg_vol) if avg_vol > 0 else 1.0

        # Distance from 52-week high
        high_52w      = float(high.tail(252).max())
        pct_from_high = (price - high_52w) / high_52w * 100

        # 20-day momentum
        momentum_20d = float((price - float(close.iloc[-20])) / float(close.iloc[-20]) * 100)

        # Consolidation: prior ATR vs recent ATR — higher = tighter recently (coiling)
        atr_recent = float((high - low).tail(10).mean())
        atr_prior  = float((high - low).tail(30).head(20).mean())
        consolidation = atr_prior / atr_recent if atr_recent > 0 else 1.0

        # Short interest (slower call — only on shortlisted stocks)
        info      = yf.Ticker(ticker).info
        short_pct = float(info.get("shortPercentOfFloat") or 0)
        float_m   = (info.get("floatShares") or info.get("sharesOutstanding") or 0) / 1e6

        # ── Scoring ───────────────────────────────────────────────────────────
        score   = 0.0
        reasons = []

        # Trend
        if price > ma20:
            score += 20
            reasons.append("above MA20")
        if ma20 > ma50:
            score += 15
            reasons.append("MA20>MA50")
        if price > ma50:
            score += 10

        # RSI sweet spot: 50–70 = momentum without being overbought
        if 50 <= rsi <= 70:
            score += 25
            reasons.append(f"RSI {rsi:.0f}")
        elif 45 <= rsi < 50:
            score += 10
            reasons.append(f"RSI {rsi:.0f} (near)")
        elif rsi > 75:
            score -= 15
            reasons.append(f"RSI {rsi:.0f} overbought")

        # Recent volume accumulation
        if vol_ratio > 1.5:
            score += 20
            reasons.append(f"vol {vol_ratio:.1f}x")
        elif vol_ratio > 1.2:
            score += 10
            reasons.append(f"vol {vol_ratio:.1f}x")

        # Near 52-week high = breakout territory
        if pct_from_high >= -5:
            score += 20
            reasons.append("near 52w high")
        elif pct_from_high >= -15:
            score += 10

        # Medium-term momentum
        if momentum_20d > 15:
            score += 20
            reasons.append(f"+{momentum_20d:.0f}% 20d")
        elif momentum_20d > 7:
            score += 12
            reasons.append(f"+{momentum_20d:.0f}% 20d")
        elif momentum_20d > 3:
            score += 5

        # Consolidation / coiling
        if consolidation > 1.4:
            score += 15
            reasons.append("tight coil")
        elif consolidation > 1.15:
            score += 8

        # Short squeeze potential
        if short_pct > 0.20:
            score += 20
            reasons.append(f"{short_pct*100:.0f}% shorted")
        elif short_pct > 0.10:
            score += 10
            reasons.append(f"{short_pct*100:.0f}% shorted")

        # ── Hard disqualifiers ────────────────────────────────────────────────
        if price < ma50 and momentum_20d < 0:
            return None   # clear downtrend
        if rsi < 35:
            return None   # broken / falling knife

        return {
            "ticker":        ticker,
            "price":         price,
            "score":         score,
            "rsi":           rsi,
            "ma20":          ma20,
            "ma50":          ma50,
            "above_ma20":    price > ma20,
            "above_ma50":    price > ma50,
            "vol_ratio":     vol_ratio,
            "pct_from_high": pct_from_high,
            "momentum_20d":  momentum_20d,
            "consolidation": consolidation,
            "short_pct":     short_pct,
            "float_m":       float_m,
            "shares":        int(ORDER_USD / price),
            "reasons":       reasons,
        }
    except Exception:
        return None


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 65)
    print(f"  Swing Trade Scanner  —  {datetime.now().strftime('%b %d %Y  %H:%M')}")
    print(f"  Scoring for 3-5 day holds  |  Position: ${ORDER_USD:,.0f}")
    print("=" * 65 + "\n")

    universe = get_universe()
    print(f"Scanning {len(universe)} tickers …\n")

    results = []
    for ticker in universe:
        print(f"  {ticker} …", end="\r")
        r = analyze(ticker)
        if r:
            results.append(r)

    results.sort(key=lambda x: x["score"], reverse=True)

    if not results:
        print("No qualifying swing setups found.\n")
        return

    print(f"\nTop {min(TOP_N, len(results))} swing trade picks:\n")
    header = f"{'#':<3} {'Ticker':<7} {'Price':>7} {'Score':>6} {'RSI':>5} " \
             f"{'Vol':>5} {'20dMom':>7} {'FromHi':>8}  Reasons"
    print(header)
    print("─" * 90)

    for i, r in enumerate(results[:TOP_N], 1):
        trend = "↑" if r["above_ma20"] and r["above_ma50"] else \
                ("→" if r["above_ma50"] else "↓")
        print(f"{i:<3} {r['ticker']:<7} ${r['price']:>6.2f} {r['score']:>6.0f} "
              f"{r['rsi']:>5.0f} {r['vol_ratio']:>4.1f}x "
              f"{r['momentum_20d']:>+6.1f}% {r['pct_from_high']:>+7.1f}%  "
              f"{trend} {', '.join(r['reasons'])}")

    top = results[0]
    print(f"\n{'─' * 65}")
    print(f"  #1 Pick:  {top['ticker']}  —  {top['shares']:,} shares @ ${top['price']:.2f}")
    print(f"  Score:    {top['score']:.0f}/145")
    print(f"  Why:      {', '.join(top['reasons'])}")
    if top["float_m"] > 0:
        print(f"  Float:    {top['float_m']:.0f}M shares")
    if top["short_pct"] > 0:
        print(f"  Short %:  {top['short_pct']*100:.0f}% of float")
    print()


if __name__ == "__main__":
    main()
