
"""
MarketWatch VSE  —  Strategy Backtester
=========================================
Replays the last N trading days through all 8 strategies using real
historical data from yfinance. No browser, no real orders — pure simulation.

What it tests
─────────────
[1] Intraday Momentum    — buys morning movers, holds until stop/target/EOD
[2] Extreme Fade         — shorts/longs extreme daily movers overnight
[3] Short Squeeze        — buys high-short-float stocks already spiking
[4] Pre-Market Gap       — longs gap-ups / shorts gap-downs, exits same day at stop/target/close
[5] News Catalyst        — simulated via big-move + volume surge proxy
[6] Earnings Catalyst    — buy day before known earnings, sell at next open
[7] UVXY Decay Short     — short UVXY Monday, cover Friday
[8] Sector Rotation      — buy laggards when sector leader spikes

Output
──────
• Per-trade log with entry/exit/P&L
• Per-strategy summary (win rate, avg return, total P&L, Sharpe-like ratio)
• Overall portfolio equity curve (printed as ASCII chart)
• Best and worst trades

Run
───
pip install yfinance pandas numpy
python backtest.py
"""

import logging
import re
import requests
from collections import defaultdict
from datetime import date, timedelta

import numpy as np
import pandas as pd
import yfinance as yf

# ═══════════════════════════════════════════════════════════════
#  BACKTEST CONFIG
# ═══════════════════════════════════════════════════════════════
BT = {
    # ── Simulation window ─────────────────────────────────────
    "lookback_days":        44,     # ~2 months of trading days
    "starting_capital":  500_000,   # VSE game starting balance ($)
    "backtest_start":  "2025-01-02",  # historical test window start
    "backtest_end":    "2025-02-28",  # historical test window end

    # ── Strategy toggles ──────────────────────────────────────
    "run_momentum":   False,
    "run_fade":       False,
    "run_squeeze":    False,
    "run_gap":        True,
    "run_news":       False,
    "run_earnings":   False,
    "run_uvxy":       False,
    "run_sector":     False,

    # ── Position sizing ───────────────────────────────────────
    "momentum_order_usd":   15_000,
    "fade_order_usd":       25_000,
    "squeeze_order_usd":    20_000,
    "gap_order_usd":       200_000,
    "news_order_usd":       15_000,
    "earnings_order_usd":   20_000,
    "uvxy_order_usd":       20_000,
    "sector_order_usd":     15_000,

    # ── Strategy parameters ───────────────────────────────────
    # Momentum
    "momentum_top_n":        3,
    "momentum_min_pct":      3.0,
    "momentum_min_relvol":   2.0,
    "momentum_stop_pct":    -5.0,
    "momentum_target_pct":  10.0,

    # Fade
    "fade_max_positions":    4,
    "fade_short_min_pct":   50.0,
    "fade_long_max_pct":   -50.0,
    "fade_min_price":        1.0,
    "fade_max_price":      500.0,
    "fade_min_volume":     500_000,
    "fade_stop_pct":        15.0,
    "fade_target_pct":      20.0,

    # Squeeze
    "squeeze_top_n":         2,
    "squeeze_min_pct":       5.0,
    "squeeze_min_relvol":    3.0,
    "squeeze_min_short_float": 20.0,
    "squeeze_stop_pct":     -8.0,
    "squeeze_target_pct":   25.0,

    # Gap
    "gap_top_n":             1,     # max concurrent longs AND shorts (each)
    "gap_min_pct":           5.0,   # long: gap-up at least this %
    "gap_max_pct":          50.0,   # long: gap-up at most this %
    "gap_stop_pct":         -6.0,   # loss limit (applied to both sides)
    "gap_target_pct":       15.0,   # profit target (applied to both sides)
    "gap_hold_bars":         1,     # in daily bars (1 = same day exit)
    "gap_delay_fraction":    0.0,   # enter at open (9:30)
    "gap_min_relvol":        1.5,   # only trade if volume is already surging at open
    "gap_short_min_pct":     5.0,   # short: gap-down at least this % (absolute)
    "gap_short_max_pct":    50.0,   # short: gap-down at most this % (absolute)

    # News (proxy: big move + volume surge, no real news feed in backtest)
    "news_top_n":            2,
    "news_min_pct":          5.0,
    "news_min_relvol":       4.0,   # very high volume = likely news-driven
    "news_stop_pct":        -6.0,
    "news_target_pct":      15.0,

    # Earnings
    "earnings_stop_pct":    -8.0,
    "earnings_min_atr_pct":  4.0,

    # UVXY
    "uvxy_vix_max":         25.0,
    "uvxy_stop_pct":        12.0,
    "uvxy_weekly_target_pct": 6.0,

    # Sector
    "sector_top_n":          2,
    "sector_leader_min_pct": 5.0,
    "sector_lag_max_pct":    2.0,
    "sector_stop_pct":      -5.0,
    "sector_target_pct":     8.0,
    "sector_hold_days":      2,

    # ── Live movers (scraped from stockanalysis.com at runtime) ──
    "live_movers_enabled":   True,
    "live_movers_top_n":     15,    # top N gainers + top N losers to add each morning
    "live_movers_min_price":  2.0,  # filter out penny stocks
    "live_movers_min_volume": 300_000,

    # ── Universes ─────────────────────────────────────────────
    "momentum_universe": [
        "NVDA","TSLA","META","AMD","MSFT","AAPL","AMZN","GOOGL",
        "SMCI","ARM","AVGO","MRVL","QCOM","MU",
        "TQQQ","SOXL","FNGU","TECL",
        "GME","AMC","PLTR","MSTR","SOFI","RIVN",
        "SAVA","SRPT","ARKG",
    ],
    "squeeze_universe": [
        "GME","AMC","MSTR","RIVN","LCID","NKLA",
        "HOOD","SOFI","COIN","RBLX","PLTR","CVNA","UPST",
        "PTON","OPEN","SPCE","SAVA","SRPT","BLUE",
    ],
    "gap_universe": [
        # Mega-cap movers
        "NVDA","TSLA","META","AMD","MSFT","AAPL","AMZN","GOOGL",
        # High-beta tech / semis
        "SMCI","ARM","AVGO","MRVL","QCOM","MU","INTC","CRWD","NET","DDOG",
        # Crypto / fintech
        "COIN","MSTR","HOOD","MARA","RIOT","CLSK","CIFR","SOFI","UPST",
        # Meme / high short-interest
        "GME","AMC","PLTR","RIVN","LCID","BBAI","RKLB",
        # Biotech (big catalyst movers)
        "MRNA","BNTX","NVAX","SRPT","SAVA","ARKG","AGEN","ACMR","ALNY",
        # Leveraged ETFs (extreme intraday moves)
        "TQQQ","SOXL","FNGU","TECL","LABU","WEBL","NAIL","CURE",
        # Misc volatile
        "RBLX","SNAP","U","OPEN","PTON","SPCE","JOBY","ACHR",
    ],
    "earnings_universe": [
        "NVDA","TSLA","META","AMD","MSFT","AAPL","AMZN","GOOGL",
        "NFLX","COIN","SNAP","RBLX","PLTR",
        "MRNA","BNTX","SRPT","SMCI","ARM","AVGO",
    ],
    "sector_map": {
        "NVDA":  ["AMD","SMCI","MRVL","AVGO","MU"],
        "TSLA":  ["RIVN","LCID"],
        "MSTR":  ["COIN","HOOD"],
        "META":  ["SNAP","PINS","RBLX"],
        "MRNA":  ["BNTX","NVAX","SAVA","SRPT"],
        "TQQQ":  ["SOXL","TECL","FNGU"],
        "GME":   ["AMC"],
        "PLTR":  ["SOFI","HOOD"],
    },
}

logging.basicConfig(level=logging.WARNING)   # suppress yfinance noise
log = logging.getLogger("backtest")
log.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(message)s"))
log.addHandler(handler)
log.propagate = False


# ═══════════════════════════════════════════════════════════════
#  LIVE MOVERS SCRAPER
# ═══════════════════════════════════════════════════════════════

def fetch_live_movers(cfg: dict) -> list[str]:
    """Scrape stockanalysis.com for today's top gainers and losers.
    Returns filtered ticker symbols to append to the gap universe.
    """
    if not cfg.get("live_movers_enabled", True):
        return []

    min_price = cfg.get("live_movers_min_price", 2.0)
    min_vol   = cfg.get("live_movers_min_volume", 300_000)
    top_n     = cfg.get("live_movers_top_n", 15)
    headers   = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    # Data is in HTML table rows: ticker in <a href="/stocks/xx/">, then change%, price, volume in <td>s
    _ROW_RE = re.compile(
        r'href="/stocks/[^/]+/">([A-Z]{1,5})</a>'   # ticker symbol
        r'.*?[\d.]+%'                                 # skip change% cell
        r'.*?<td[^>]*>([\d.]+)</td>'                 # price
        r'.*?<td[^>]*>([\d,]+)</td>',                # volume
        re.DOTALL
    )

    all_tickers: list[str] = []
    for label, url in [
        ("gainers", "https://stockanalysis.com/markets/gainers/"),
        ("losers",  "https://stockanalysis.com/markets/losers/"),
    ]:
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            found: list[str] = []
            for m in _ROW_RE.finditer(resp.text):
                sym, price, vol = m.group(1), float(m.group(2)), int(m.group(3).replace(",", ""))
                if price >= min_price and vol >= min_vol and sym not in found:
                    found.append(sym)
                    if len(found) >= top_n:
                        break
            log.info("Live %-7s (%d tickers): %s", label, len(found), " ".join(found))
            all_tickers.extend(found)
        except Exception as exc:
            log.warning("Could not fetch live movers from %s: %s", label, exc)

    # Deduplicate preserving order
    seen: set[str] = set()
    return [t for t in all_tickers if not (t in seen or seen.add(t))]  # type: ignore[func-returns-value]


# ═══════════════════════════════════════════════════════════════
#  DATA LOADER
# ═══════════════════════════════════════════════════════════════

def load_all_data(cfg: dict) -> dict[str, pd.DataFrame]:
    """Download daily OHLCV for every ticker we need. Returns {ticker: df}."""
    all_tickers = set(cfg["momentum_universe"])
    all_tickers |= set(cfg["squeeze_universe"])
    all_tickers |= set(cfg["gap_universe"])
    all_tickers |= set(cfg["earnings_universe"])
    all_tickers |= set(cfg["sector_map"].keys())
    for lags in cfg["sector_map"].values():
        all_tickers |= set(lags)
    all_tickers |= {"UVXY","^VIX","SPY"}

    all_tickers = sorted(all_tickers)
    log.info("Downloading %d tickers …", len(all_tickers))

    # Use explicit date range if provided, else rolling period
    if "backtest_start" in cfg and "backtest_end" in cfg:
        from datetime import datetime, timedelta as td
        buf_start = (datetime.strptime(cfg["backtest_start"], "%Y-%m-%d") - td(days=60)).strftime("%Y-%m-%d")
        raw = yf.download(
            all_tickers, start=buf_start, end=cfg["backtest_end"],
            interval="1d", auto_adjust=True, progress=False, group_by="ticker",
        )
    else:
        period = f"{cfg['lookback_days'] + 40}d"
        raw = yf.download(
            all_tickers, period=period, interval="1d",
            auto_adjust=True, progress=False, group_by="ticker",
        )

    data = {}
    for ticker in all_tickers:
        try:
            df = (raw if len(all_tickers) == 1 else raw[ticker]).dropna(how="all")
            if len(df) >= 10:
                data[ticker] = df
        except Exception:
            pass

    log.info("Loaded %d tickers successfully.\n", len(data))
    return data


def get_trading_days(data: dict, cfg: dict) -> list[date]:
    """Extract trading days from SPY within the configured window."""
    spy = data.get("SPY", next(iter(data.values())))
    all_days = [d.date() for d in spy.index]
    if "backtest_start" in cfg and "backtest_end" in cfg:
        from datetime import datetime
        start = datetime.strptime(cfg["backtest_start"], "%Y-%m-%d").date()
        end   = datetime.strptime(cfg["backtest_end"],   "%Y-%m-%d").date()
        return [d for d in all_days if start <= d <= end]
    return all_days[-cfg["lookback_days"]:]


# ═══════════════════════════════════════════════════════════════
#  HELPERS  — historical slices
# ═══════════════════════════════════════════════════════════════

def price_on(data: dict, ticker: str, day: date) -> float | None:
    if ticker not in data:
        return None
    df = data[ticker]
    idx = [d.date() for d in df.index]
    if day in idx:
        return float(df.loc[df.index[idx.index(day)], "Close"])
    return None


def open_on(data: dict, ticker: str, day: date) -> float | None:
    if ticker not in data:
        return None
    df = data[ticker]
    idx = [d.date() for d in df.index]
    if day in idx:
        return float(df.loc[df.index[idx.index(day)], "Open"])
    return None


def change_pct_on(data: dict, ticker: str, day: date) -> float | None:
    """% change from previous close to this day's close."""
    if ticker not in data:
        return None
    df   = data[ticker]
    days = [d.date() for d in df.index]
    if day not in days:
        return None
    i = days.index(day)
    if i < 1:
        return None
    prev  = float(df["Close"].iloc[i-1])
    today = float(df["Close"].iloc[i])
    return (today - prev) / prev * 100 if prev > 0 else None


def gap_pct_on(data: dict, ticker: str, day: date) -> float | None:
    """Today's open vs yesterday's close."""
    if ticker not in data:
        return None
    df   = data[ticker]
    days = [d.date() for d in df.index]
    if day not in days:
        return None
    i = days.index(day)
    if i < 1:
        return None
    prev_close = float(df["Close"].iloc[i-1])
    today_open = float(df["Open"].iloc[i])
    return (today_open - prev_close) / prev_close * 100 if prev_close > 0 else None


def rel_volume_on(data: dict, ticker: str, day: date, window: int = 20) -> float:
    if ticker not in data:
        return 0.0
    df   = data[ticker]
    days = [d.date() for d in df.index]
    if day not in days:
        return 0.0
    i = days.index(day)
    if i < 1:
        return 0.0
    avg = df["Volume"].iloc[max(0, i-window):i].mean()
    return float(df["Volume"].iloc[i]) / avg if avg > 0 else 0.0


def atr_pct_on(data: dict, ticker: str, day: date, period: int = 14) -> float:
    if ticker not in data:
        return 0.0
    df   = data[ticker]
    days = [d.date() for d in df.index]
    if day not in days:
        return 0.0
    i = days.index(day)
    if i < period:
        return 0.0
    sub = df.iloc[i-period:i]
    hi, lo, cl = sub["High"], sub["Low"], sub["Close"]
    tr = pd.concat([
        hi - lo,
        (hi - cl.shift()).abs(),
        (lo - cl.shift()).abs()
    ], axis=1).max(axis=1)
    price = float(df["Close"].iloc[i])
    return float(tr.mean() / price * 100) if price > 0 else 0.0


def next_trading_day(days: list[date], day: date) -> date | None:
    later = [d for d in days if d > day]
    return later[0] if later else None


def day_of_week(day: date) -> int:
    return day.weekday()  # 0=Mon, 4=Fri


# ═══════════════════════════════════════════════════════════════
#  TRADE RECORD
# ═══════════════════════════════════════════════════════════════

class Trade:
    def __init__(self, strategy, ticker, side, entry_date, entry_price, shares, reason=""):
        self.strategy    = strategy
        self.ticker      = ticker
        self.side        = side          # "LONG" or "SHORT"
        self.entry_date  = entry_date
        self.entry_price = entry_price
        self.shares      = shares
        self.reason      = reason
        self.exit_date   = None
        self.exit_price  = None
        self.exit_reason = None

    @property
    def pnl(self) -> float:
        if self.exit_price is None:
            return 0.0
        if self.side == "SHORT":
            return (self.entry_price - self.exit_price) * self.shares
        return (self.exit_price - self.entry_price) * self.shares

    @property
    def pnl_pct(self) -> float:
        if self.exit_price is None or self.entry_price == 0:
            return 0.0
        if self.side == "SHORT":
            return (self.entry_price - self.exit_price) / self.entry_price * 100
        return (self.exit_price - self.entry_price) / self.entry_price * 100

    @property
    def closed(self) -> bool:
        return self.exit_price is not None

    def close(self, day: date, price: float, reason: str):
        self.exit_date   = day
        self.exit_price  = round(price, 4)
        self.exit_reason = reason

    def __repr__(self):
        status = (f"→ {self.exit_date} ${self.exit_price:.2f} "
                  f"[{self.exit_reason}] P&L=${self.pnl:+,.0f} ({self.pnl_pct:+.1f}%)"
                  if self.closed else "(open)")
        return (f"{self.strategy:<12} {self.side:<5} {self.ticker:<6}  "
                f"entry={self.entry_date} ${self.entry_price:.2f}  {status}")


# ═══════════════════════════════════════════════════════════════
#  BACKTEST ENGINE
# ═══════════════════════════════════════════════════════════════

class Backtester:
    def __init__(self, cfg: dict, data: dict, trading_days: list[date]):
        self.cfg          = cfg
        self.data         = data
        self.days         = trading_days
        self.capital      = cfg["starting_capital"]
        self.trades: list[Trade] = []
        self.open_trades: list[Trade] = []
        self.daily_equity: list[tuple[date, float]] = []

    # ── helpers ──────────────────────────────────────────────

    def _held(self) -> set[str]:
        return {t.ticker for t in self.open_trades}

    def _open_trade(self, strategy, ticker, side, day, price, order_usd, reason=""):
        if price <= 0:
            return
        shares = max(1, int(order_usd / price))
        cost   = shares * price
        if cost > self.capital:
            shares = max(1, int(self.capital * 0.9 / price))
        t = Trade(strategy, ticker, side, day, price, shares, reason)
        self.open_trades.append(t)
        self.trades.append(t)

    def _close_trade(self, trade: Trade, day: date, price: float, reason: str):
        trade.close(day, price, reason)
        self.open_trades.remove(trade)
        self.capital += trade.pnl

    def _check_stops(self, day: date,
                     stop_pct: float, target_pct: float,
                     strategy: str):
        """Close open trades for a strategy that hit stop or target today."""
        exits = []
        for t in self.open_trades:
            if t.strategy != strategy:
                continue
            price = price_on(self.data, t.ticker, day)
            if price is None:
                continue
            pnl_pct = t.pnl_pct if t.exit_price is None else 0
            # Compute live pnl_pct
            if t.side == "SHORT":
                live_pnl = (t.entry_price - price) / t.entry_price * 100
            else:
                live_pnl = (price - t.entry_price) / t.entry_price * 100

            if live_pnl <= stop_pct:
                exits.append((t, price, f"STOP {live_pnl:.1f}%"))
            elif live_pnl >= target_pct:
                exits.append((t, price, f"TARGET +{live_pnl:.1f}%"))

        for t, price, reason in exits:
            self._close_trade(t, day, price, reason)

    def _close_strategy_eod(self, day: date, strategy: str):
        """Flatten all open trades for a strategy at today's close."""
        for t in list(self.open_trades):
            if t.strategy != strategy:
                continue
            price = price_on(self.data, t.ticker, day)
            if price is None:
                price = t.entry_price
            if t.side == "SHORT":
                pnl = (t.entry_price - price) / t.entry_price * 100
            else:
                pnl = (price - t.entry_price) / t.entry_price * 100
            self._close_trade(t, day, price, f"EOD {pnl:+.1f}%")

    # ── strategy simulators ──────────────────────────────────

    def sim_momentum(self, day: date):
        cfg = self.cfg
        if not cfg["run_momentum"]:
            return
        # Check stops first
        self._check_stops(day, cfg["momentum_stop_pct"],
                          cfg["momentum_target_pct"], "momentum")
        # Score universe
        candidates = []
        for ticker in cfg["momentum_universe"]:
            if ticker in self._held():
                continue
            chg = change_pct_on(self.data, ticker, day)
            rv  = rel_volume_on(self.data, ticker, day)
            if chg is None:
                continue
            if chg >= cfg["momentum_min_pct"] and rv >= cfg["momentum_min_relvol"]:
                score = chg * 2 + rv * 1.5
                price = price_on(self.data, ticker, day)
                if price and price > 0:
                    candidates.append((score, ticker, price, chg, rv))

        candidates.sort(reverse=True)
        slots = cfg["momentum_top_n"] - sum(1 for t in self.open_trades
                                             if t.strategy == "momentum")
        for score, ticker, price, chg, rv in candidates[:slots]:
            self._open_trade("momentum", ticker, "LONG", day, price,
                             cfg["momentum_order_usd"],
                             f"{chg:+.1f}% vol={rv:.1f}x score={score:.1f}")

        # EOD close
        self._close_strategy_eod(day, "momentum")


    def sim_fade(self, day: date, next_day: date | None):
        cfg = self.cfg
        if not cfg["run_fade"]:
            return

        # Exit previous night's fades at today's open
        for t in list(self.open_trades):
            if t.strategy != "fade":
                continue
            if t.entry_date >= day:
                continue
            price = open_on(self.data, t.ticker, day) or price_on(self.data, t.ticker, day)
            if price is None:
                price = t.entry_price
            if t.side == "SHORT":
                pnl = (t.entry_price - price) / t.entry_price * 100
            else:
                pnl = (price - t.entry_price) / t.entry_price * 100
            reason = (f"TARGET +{pnl:.1f}%" if pnl >= cfg["fade_target_pct"]
                      else f"STOP {pnl:.1f}%" if pnl <= -cfg["fade_stop_pct"]
                      else f"OPEN EXIT {pnl:+.1f}%")
            self._close_trade(t, day, price, reason)

        if next_day is None:
            return

        # Enter new fades at today's close
        shorts_entered = longs_entered = 0
        for ticker in set(cfg["momentum_universe"]) | set(cfg["squeeze_universe"]):
            if ticker in self._held():
                continue
            chg  = change_pct_on(self.data, ticker, day)
            vol  = self.data.get(ticker, pd.DataFrame())
            if chg is None or vol.empty:
                continue
            days_list = [d.date() for d in vol.index]
            if day not in days_list:
                continue
            i = days_list.index(day)
            today_vol = int(vol["Volume"].iloc[i])
            price = price_on(self.data, ticker, day)
            if price is None or price < cfg["fade_min_price"] or price > cfg["fade_max_price"]:
                continue
            if today_vol < cfg["fade_min_volume"]:
                continue

            if chg >= cfg["fade_short_min_pct"] and \
               shorts_entered < cfg["fade_max_positions"]:
                self._open_trade("fade", ticker, "SHORT", day, price,
                                 cfg["fade_order_usd"],
                                 f"FADE SHORT +{chg:.0f}%")
                shorts_entered += 1

            elif chg <= cfg["fade_long_max_pct"] and \
                 longs_entered < cfg["fade_max_positions"]:
                self._open_trade("fade", ticker, "LONG", day, price,
                                 cfg["fade_order_usd"],
                                 f"FADE LONG {chg:.0f}%")
                longs_entered += 1


    def sim_squeeze(self, day: date):
        cfg = self.cfg
        if not cfg["run_squeeze"]:
            return
        self._check_stops(day, cfg["squeeze_stop_pct"],
                          cfg["squeeze_target_pct"], "squeeze")
        candidates = []
        for ticker in cfg["squeeze_universe"]:
            if ticker in self._held():
                continue
            chg = change_pct_on(self.data, ticker, day)
            rv  = rel_volume_on(self.data, ticker, day)
            price = price_on(self.data, ticker, day)
            if chg is None or price is None:
                continue
            # Proxy: use high relative volume (≥ 3×) + big move as squeeze signal
            # (no real short-float data in backtest — use vol surge as proxy)
            if chg >= cfg["squeeze_min_pct"] and rv >= cfg["squeeze_min_relvol"]:
                candidates.append((rv * chg, ticker, price, chg, rv))

        candidates.sort(reverse=True)
        slots = cfg["squeeze_top_n"] - sum(1 for t in self.open_trades
                                            if t.strategy == "squeeze")
        for _, ticker, price, chg, rv in candidates[:slots]:
            self._open_trade("squeeze", ticker, "LONG", day, price,
                             cfg["squeeze_order_usd"],
                             f"SQUEEZE proxy chg={chg:+.1f}% rv={rv:.1f}x")

        self._close_strategy_eod(day, "squeeze")


    def sim_gap(self, day: date):
        cfg = self.cfg
        if not cfg["run_gap"]:
            return
        # Gap plays: long gap-ups, short gap-downs — ranked by gap% × rel-volume
        long_candidates  = []
        short_candidates = []
        for ticker in cfg["gap_universe"]:
            if ticker in self._held():
                continue
            gap = gap_pct_on(self.data, ticker, day)
            entry_price = open_on(self.data, ticker, day)
            if gap is None or entry_price is None:
                continue
            rv = rel_volume_on(self.data, ticker, day)
            if rv < cfg.get("gap_min_relvol", 1.0):
                continue
            score = abs(gap) * rv
            if cfg["gap_min_pct"] <= gap <= cfg["gap_max_pct"]:
                long_candidates.append((score, gap, ticker, entry_price))
            elif -cfg["gap_short_max_pct"] <= gap <= -cfg["gap_short_min_pct"]:
                short_candidates.append((score, gap, ticker, entry_price))

        long_candidates.sort(reverse=True)
        short_candidates.sort(reverse=True)

        slots = cfg["gap_top_n"] - sum(1 for t in self.open_trades
                                       if t.strategy == "gap")
        delay = cfg.get("gap_delay_fraction", 0.0)
        for candidates, side in ((long_candidates, "LONG"), (short_candidates, "SHORT")):
            for score, gap, ticker, open_price in candidates[:slots]:
                # Enter at open (9:30); delay fraction = 0 so entry_price == open_price
                close_price = price_on(self.data, ticker, day) or open_price
                entry_price = open_price + (close_price - open_price) * delay
                rv = rel_volume_on(self.data, ticker, day)
                arrow = "↑" if side == "LONG" else "↓"
                self._open_trade("gap", ticker, side, day, entry_price,
                                 cfg["gap_order_usd"],
                                 f"GAP {arrow}{gap:+.1f}% rv={rv:.1f}x score={score:.1f}")

        # Exit at close (or stop/target using close as proxy) — side-aware P&L
        for t in list(self.open_trades):
            if t.strategy != "gap" or t.entry_date != day:
                continue
            close_price = price_on(self.data, t.ticker, day)
            if close_price is None:
                close_price = t.entry_price
            if t.side == "SHORT":
                pnl = (t.entry_price - close_price) / t.entry_price * 100
            else:
                pnl = (close_price - t.entry_price) / t.entry_price * 100
            reason = (f"STOP {pnl:.1f}%"      if pnl <= cfg["gap_stop_pct"]
                      else f"TARGET +{pnl:.1f}%" if pnl >= cfg["gap_target_pct"]
                      else f"TIME {pnl:+.1f}%")
            self._close_trade(t, day, close_price, reason)


    def sim_news(self, day: date):
        cfg = self.cfg
        if not cfg["run_news"]:
            return
        self._check_stops(day, cfg["news_stop_pct"],
                          cfg["news_target_pct"], "news")
        candidates = []
        universe = list(set(cfg["momentum_universe"]) | set(cfg["gap_universe"]))
        for ticker in universe:
            if ticker in self._held():
                continue
            chg = change_pct_on(self.data, ticker, day)
            rv  = rel_volume_on(self.data, ticker, day)
            price = price_on(self.data, ticker, day)
            if chg is None or price is None:
                continue
            if chg >= cfg["news_min_pct"] and rv >= cfg["news_min_relvol"]:
                candidates.append((rv, ticker, price, chg, rv))

        candidates.sort(reverse=True)
        slots = cfg["news_top_n"] - sum(1 for t in self.open_trades
                                         if t.strategy == "news")
        for _, ticker, price, chg, rv in candidates[:slots]:
            self._open_trade("news", ticker, "LONG", day, price,
                             cfg["news_order_usd"],
                             f"NEWS proxy chg={chg:+.1f}% rv={rv:.1f}x")

        self._close_strategy_eod(day, "news")


    def sim_earnings(self, day: date, all_days: list[date]):
        cfg = self.cfg
        if not cfg["run_earnings"]:
            return

        # Exit previous earnings positions at today's open
        for t in list(self.open_trades):
            if t.strategy != "earnings" or t.entry_date >= day:
                continue
            price = open_on(self.data, t.ticker, day) or price_on(self.data, t.ticker, day)
            if price is None:
                price = t.entry_price
            pnl = (price - t.entry_price) / t.entry_price * 100
            self._close_trade(t, day, price, f"EARNINGS EXIT {pnl:+.1f}%")

        next_d = next_trading_day(all_days, day)
        if next_d is None:
            return

        for ticker in cfg["earnings_universe"]:
            if ticker in self._held():
                continue
            atr = atr_pct_on(self.data, ticker, day)
            if atr < cfg["earnings_min_atr_pct"]:
                continue
            recent = [t for t in self.trades
                      if t.strategy == "earnings" and t.ticker == ticker
                      and t.entry_date is not None
                      and (day - t.entry_date).days < 20]
            if recent:
                continue

            next_chg = change_pct_on(self.data, ticker, next_d)
            if next_chg is None or abs(next_chg) < 5.0:
                continue

            price = price_on(self.data, ticker, day)
            if price is None:
                continue
            self._open_trade("earnings", ticker, "LONG", day, price,
                             cfg["earnings_order_usd"],
                             f"EARNINGS eve ATR={atr:.1f}%")


    def sim_uvxy(self, day: date, all_days: list[date]):
        cfg = self.cfg
        if not cfg["run_uvxy"]:
            return

        dow = day_of_week(day)
        vix_df = self.data.get("^VIX")

        def get_vix(d: date) -> float:
            if vix_df is None:
                return 18.0
            days_list = [dd.date() for dd in vix_df.index]
            if d not in days_list:
                return 18.0
            return float(vix_df["Close"].iloc[days_list.index(d)])

        if dow == 0 and "UVXY" not in self._held():
            vix = get_vix(day)
            if vix <= cfg["uvxy_vix_max"]:
                price = price_on(self.data, "UVXY", day)
                if price:
                    self._open_trade("uvxy", "UVXY", "SHORT", day, price,
                                     cfg["uvxy_order_usd"],
                                     f"UVXY SHORT VIX={vix:.1f}")

        for t in list(self.open_trades):
            if t.strategy != "uvxy":
                continue
            price = price_on(self.data, "UVXY", day)
            if price is None:
                continue
            pnl = (t.entry_price - price) / t.entry_price * 100
            if pnl <= -cfg["uvxy_stop_pct"]:
                self._close_trade(t, day, price, f"UVXY STOP {pnl:.1f}%")
            elif pnl >= cfg["uvxy_weekly_target_pct"] and dow == 4:
                self._close_trade(t, day, price, f"UVXY TARGET +{pnl:.1f}%")

        if dow == 4:
            for t in list(self.open_trades):
                if t.strategy != "uvxy":
                    continue
                price = price_on(self.data, "UVXY", day)
                if price is None:
                    price = t.entry_price
                pnl = (t.entry_price - price) / t.entry_price * 100
                self._close_trade(t, day, price, f"UVXY WEEKLY COVER {pnl:+.1f}%")


    def sim_sector(self, day: date, all_days: list[date]):
        cfg = self.cfg
        if not cfg["run_sector"]:
            return
        self._check_stops(day, cfg["sector_stop_pct"],
                          cfg["sector_target_pct"], "sector")

        for t in list(self.open_trades):
            if t.strategy != "sector":
                continue
            days_held = sum(1 for d in all_days
                            if t.entry_date <= d <= day)
            if days_held >= cfg["sector_hold_days"]:
                price = price_on(self.data, t.ticker, day)
                if price:
                    pnl = (price - t.entry_price) / t.entry_price * 100
                    self._close_trade(t, day, price,
                                      f"SECTOR TIME {pnl:+.1f}%")

        entered = 0
        for leader, laggards in cfg["sector_map"].items():
            leader_chg = change_pct_on(self.data, leader, day)
            if leader_chg is None or leader_chg < cfg["sector_leader_min_pct"]:
                continue
            for lag in laggards:
                if lag in self._held() or entered >= cfg["sector_top_n"]:
                    continue
                lag_chg = change_pct_on(self.data, lag, day)
                price   = price_on(self.data, lag, day)
                if lag_chg is None or price is None:
                    continue
                if lag_chg <= cfg["sector_lag_max_pct"]:
                    self._open_trade("sector", lag, "LONG", day, price,
                                     cfg["sector_order_usd"],
                                     f"ROTATION {leader}+{leader_chg:.1f}% lag={lag_chg:.1f}%")
                    entered += 1


    # ── main loop ────────────────────────────────────────────

    def run(self) -> list[Trade]:
        log.info("Running backtest over %d trading days …\n", len(self.days))

        for i, day in enumerate(self.days):
            next_day = self.days[i+1] if i+1 < len(self.days) else None

            self.sim_momentum(day)
            self.sim_fade(day, next_day)
            self.sim_squeeze(day)
            self.sim_gap(day)
            self.sim_news(day)
            self.sim_earnings(day, self.days)
            self.sim_uvxy(day, self.days)
            self.sim_sector(day, self.days)

            unrealised = 0.0
            for t in self.open_trades:
                p = price_on(self.data, t.ticker, day)
                if p:
                    if t.side == "SHORT":
                        unrealised += (t.entry_price - p) * t.shares
                    else:
                        unrealised += (p - t.entry_price) * t.shares
            self.daily_equity.append((day, self.capital + unrealised))

        last_day = self.days[-1]
        for t in list(self.open_trades):
            price = price_on(self.data, t.ticker, last_day) or t.entry_price
            pnl   = ((t.entry_price - price) if t.side == "SHORT"
                     else (price - t.entry_price)) / t.entry_price * 100
            self._close_trade(t, last_day, price, f"BACKTEST END {pnl:+.1f}%")

        return self.trades


# ═══════════════════════════════════════════════════════════════
#  REPORTING
# ═══════════════════════════════════════════════════════════════

def report(trades: list[Trade], cfg: dict, daily_equity: list[tuple]):
    closed = [t for t in trades if t.closed]
    if not closed:
        log.info("No trades closed during backtest window.")
        return

    DIVIDER  = "═" * 72
    DIVIDER2 = "─" * 72

    print(f"\n{DIVIDER}")
    print("  BACKTEST RESULTS")
    print(DIVIDER)

    strats = defaultdict(list)
    for t in closed:
        strats[t.strategy].append(t)

    print(f"\n{'Strategy':<14} {'Trades':>6} {'Win%':>6} {'Avg%':>7} "
          f"{'TotalP&L':>12} {'Best':>8} {'Worst':>8}")
    print(DIVIDER2)

    overall_pnl = 0.0
    for name in ["momentum","fade","squeeze","gap","news","earnings","uvxy","sector"]:
        ts = strats.get(name, [])
        if not ts:
            print(f"{name:<14} {'—':>6}")
            continue
        wins     = sum(1 for t in ts if t.pnl > 0)
        win_rate = wins / len(ts) * 100
        avg_pct  = np.mean([t.pnl_pct for t in ts])
        total    = sum(t.pnl for t in ts)
        best     = max(t.pnl_pct for t in ts)
        worst    = min(t.pnl_pct for t in ts)
        overall_pnl += total
        print(f"{name:<14} {len(ts):>6} {win_rate:>5.0f}% {avg_pct:>+6.1f}% "
              f"{total:>+12,.0f} {best:>+7.1f}% {worst:>+7.1f}%")

    print(DIVIDER2)
    start_cap = cfg["starting_capital"]
    end_cap   = start_cap + overall_pnl
    total_ret = overall_pnl / start_cap * 100
    print(f"{'TOTAL':<14} {len(closed):>6}       "
          f"           {overall_pnl:>+12,.0f}   {total_ret:>+.1f}% return")
    print(f"\nStarting capital: ${start_cap:>12,.0f}")
    print(f"Ending capital:   ${end_cap:>12,.0f}")

    print(f"\n{DIVIDER2}")
    print("  EQUITY CURVE")
    print(DIVIDER2)
    _ascii_equity_curve(daily_equity, cfg["starting_capital"])

    print(f"\n{DIVIDER2}")
    print("  TOP 5 TRADES")
    print(DIVIDER2)
    for t in sorted(closed, key=lambda x: x.pnl, reverse=True)[:5]:
        print(f"  {t}")

    print(f"\n{DIVIDER2}")
    print("  WORST 5 TRADES")
    print(DIVIDER2)
    for t in sorted(closed, key=lambda x: x.pnl)[:5]:
        print(f"  {t}")

    print(f"\n{DIVIDER2}")
    print("  RISK METRICS")
    print(DIVIDER2)
    daily_returns = []
    for i in range(1, len(daily_equity)):
        prev = daily_equity[i-1][1]
        curr = daily_equity[i][1]
        daily_returns.append((curr - prev) / prev if prev > 0 else 0)

    if daily_returns:
        sharpe   = (np.mean(daily_returns) / np.std(daily_returns) * np.sqrt(252)
                    if np.std(daily_returns) > 0 else 0)
        max_dd   = _max_drawdown([e for _, e in daily_equity])
        avg_win  = np.mean([t.pnl_pct for t in closed if t.pnl > 0] or [0])
        avg_loss = np.mean([t.pnl_pct for t in closed if t.pnl <= 0] or [0])
        rr_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0
        all_wins = sum(1 for t in closed if t.pnl > 0)

        print(f"  Overall win rate:      {all_wins/len(closed)*100:.1f}%  "
              f"({all_wins}/{len(closed)} trades)")
        print(f"  Avg winning trade:     {avg_win:+.1f}%")
        print(f"  Avg losing trade:      {avg_loss:+.1f}%")
        print(f"  Win/loss ratio:        {rr_ratio:.2f}x")
        print(f"  Annualised Sharpe:     {sharpe:.2f}")
        print(f"  Max drawdown:          {max_dd:.1f}%")

    print(f"\n{DIVIDER}\n")


def _max_drawdown(equity: list[float]) -> float:
    peak = equity[0]
    max_dd = 0.0
    for e in equity:
        if e > peak:
            peak = e
        dd = (peak - e) / peak * 100
        if dd > max_dd:
            max_dd = dd
    return max_dd


def _ascii_equity_curve(daily_equity: list[tuple], start_cap: float, width: int = 60, height: int = 12):
    if len(daily_equity) < 2:
        return
    values = [e for _, e in daily_equity]
    dates  = [d for d, _ in daily_equity]
    lo, hi = min(values), max(values)
    span   = hi - lo if hi > lo else 1

    rows = []
    for row in range(height, -1, -1):
        threshold = lo + (row / height) * span
        line = ""
        for v in values:
            line += "█" if v >= threshold else " "
        label = f"${threshold:>10,.0f}" if row % 3 == 0 else " " * 11
        rows.append(f"  {label} │{line[:width]}")

    n = min(width, len(values))
    x_labels = " " * 14 + "└" + "─" * n
    date_row  = " " * 15
    step = max(1, n // 4)
    for i in range(0, n, step):
        if i < len(dates):
            label = dates[i].strftime("%m/%d")
            date_row += label + " " * (step - len(label))

    for r in rows:
        print(r)
    print(x_labels)
    print(date_row)

    final   = values[-1]
    ret_pct = (final - start_cap) / start_cap * 100
    print(f"\n  Start: ${start_cap:,.0f}  →  End: ${final:,.0f}  "
          f"({ret_pct:+.1f}%)")


# ═══════════════════════════════════════════════════════════════
#  TODAY'S TRADE RECOMMENDATION
# ═══════════════════════════════════════════════════════════════

def recommend_today(cfg: dict, universe: list[str]):
    """Scan the universe with live prices and print today's best gap trade."""
    from datetime import datetime

    DIVIDER = "═" * 72

    print(f"\n{DIVIDER}")
    print(f"  TODAY'S TRADE  —  {datetime.now().strftime('%A %b %d, %Y  %H:%M')}")
    print(DIVIDER)
    print(f"  Scanning {len(universe)} tickers for gap setups …\n")

    long_candidates  = []
    short_candidates = []

    for ticker in universe:
        try:
            fi = yf.Ticker(ticker).fast_info
            prev_close = getattr(fi, "previous_close", None)
            current    = getattr(fi, "last_price",     None)
            volume     = getattr(fi, "three_month_average_volume", None)
            last_vol   = getattr(fi, "last_volume",    None)

            if not prev_close or not current or prev_close <= 0:
                continue
            if current < cfg.get("live_movers_min_price", 2.0):
                continue

            gap    = (current - prev_close) / prev_close * 100
            rel_vol = (last_vol / volume) if volume and last_vol else 0.0
            score  = abs(gap) * max(rel_vol, 0.1)  # floor rel_vol so untracked volume still shows up

            if cfg["gap_min_pct"] <= gap <= cfg["gap_max_pct"]:
                long_candidates.append((score, gap, ticker, current, rel_vol))
            elif -cfg["gap_short_max_pct"] <= gap <= -cfg["gap_short_min_pct"]:
                short_candidates.append((score, gap, ticker, current, rel_vol))
        except Exception:
            pass

    long_candidates.sort(reverse=True)
    short_candidates.sort(reverse=True)

    def print_trade(side, score, gap, ticker, price, rv):
        shares    = int(cfg["gap_order_usd"] / price)
        position  = shares * price
        stop_px   = price * (1 + cfg["gap_stop_pct"]   / 100) if side == "LONG" else price * (1 - cfg["gap_stop_pct"]   / 100)
        target_px = price * (1 + cfg["gap_target_pct"] / 100) if side == "LONG" else price * (1 - cfg["gap_target_pct"] / 100)
        arrow     = "GAP UP  " if side == "LONG" else "GAP DOWN"
        print(f"  [{side}]  {ticker}  —  {arrow} {gap:+.1f}%  |  rel-vol {rv:.1f}x  |  score {score:.1f}")
        print(f"  {'':5} Buy/Short:  {shares:,} shares @ ${price:.2f}  =  ${position:,.0f}")
        print(f"  {'':5} Stop:       ${stop_px:.2f}  ({cfg['gap_stop_pct']:+.0f}%)")
        print(f"  {'':5} Target:     ${target_px:.2f}  ({cfg['gap_target_pct']:+.0f}%)")
        print()

    if long_candidates:
        print_trade("LONG",  *long_candidates[0])
    else:
        print("  No LONG setup today (no qualifying gap-ups).\n")

    if short_candidates:
        print_trade("SHORT", *short_candidates[0])
    else:
        print("  No SHORT setup today (no qualifying gap-downs).\n")

    if not long_candidates and not short_candidates:
        print("  Sit on your hands today — no clean setups.\n")

    print(DIVIDER + "\n")


# ═══════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("\n" + "═"*72)
    print("  MarketWatch VSE  —  Strategy Backtester")
    print(f"  Lookback: {BT['lookback_days']} trading days  |  "
          f"Capital: ${BT['starting_capital']:,.0f}")
    print("═"*72 + "\n")

    # Pull today's biggest gainers + losers and add them to the scan universe
    live = fetch_live_movers(BT)
    if live:
        before = len(BT["gap_universe"])
        BT["gap_universe"] = list(dict.fromkeys(BT["gap_universe"] + live))
        log.info("Gap universe expanded: %d → %d tickers (+%d live movers)\n",
                 before, len(BT["gap_universe"]), len(BT["gap_universe"]) - before)

    data         = load_all_data(BT)
    trading_days = get_trading_days(data, BT)

    bt     = Backtester(BT, data, trading_days)
    trades = bt.run()

    report(trades, BT, bt.daily_equity)
    recommend_today(BT, BT["gap_universe"])
