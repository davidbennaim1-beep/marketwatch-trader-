"""
Run this once to save your MarketWatch login session.
A browser window will open — log in with Google as you normally would.
When you're fully logged in and can see your game, press Enter in the terminal.
"""

from playwright.sync_api import sync_playwright
import os

SESSION_FILE = os.path.join(os.path.dirname(__file__), ".mw_session")

with sync_playwright() as p:
    browser = p.webkit.launch(headless=False)
    context = browser.new_context(
        viewport={"width": 1280, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/17.0 Safari/605.1.15"
        ),
        locale="en-US",
        timezone_id="America/New_York",
    )
    context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
    """)
    page = context.new_page()
    page.goto("https://www.marketwatch.com/games/yuse-spring-2026-stock-market-competition-")

    print("\nA browser window has opened.")
    print("Log in with your Google account as you normally would.")
    print("Once you can see your game portfolio, come back here and press Enter.\n")
    input("Press Enter when logged in → ")

    context.storage_state(path=SESSION_FILE)
    browser.close()

print(f"\nSession saved to {SESSION_FILE}")
print("You can now run:  python3 trade.py\n")
