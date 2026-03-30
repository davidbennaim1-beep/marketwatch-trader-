"""
Copies your MarketWatch session cookies from Safari into the Playwright session file.
Run this while you're logged into MarketWatch in Safari.

Requirements:
    pip3 install browser-cookie3 --break-system-packages
"""

import json
import os

SESSION_FILE = os.path.join(os.path.dirname(__file__), ".mw_session")

try:
    import browser_cookie3
except ImportError:
    print("Installing browser-cookie3 …")
    os.system("pip3 install browser-cookie3 --break-system-packages")
    import browser_cookie3

print("Reading MarketWatch cookies from Safari …")
cookies = browser_cookie3.safari(domain_name=".marketwatch.com")

playwright_cookies = []
for c in cookies:
    cookie = {
        "name":   c.name,
        "value":  c.value,
        "domain": c.domain if c.domain.startswith(".") else "." + c.domain,
        "path":   c.path or "/",
        "secure": bool(c.secure),
        "httpOnly": False,
        "sameSite": "None",
    }
    if c.expires and c.expires > 0:
        cookie["expires"] = float(c.expires)
    playwright_cookies.append(cookie)

if not playwright_cookies:
    print("\nNo MarketWatch cookies found in Safari.")
    print("Make sure you're logged into marketwatch.com in Safari first.\n")
    raise SystemExit(1)

session = {"cookies": playwright_cookies, "origins": []}
with open(SESSION_FILE, "w") as f:
    json.dump(session, f)

print(f"Saved {len(playwright_cookies)} cookies to {SESSION_FILE}")
print("You can now run:  python3 trade.py\n")
