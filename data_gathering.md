📥 ### Data Gathering
We began by setting up a dedicated Python environment (`venv/conda`) and Git for version control to ensure a clean, reproducible workflow.

The core data was collected by scraping a retail and review website for electronic devices. Many of these sites relied on dynamic JavaScript rendering and anti-bot protections, which made scraping a significant challenge.

To overcome this, we used:

-`undetected-chromedriver` with Selenium for stealth automation

-`BeautifulSoup` for HTML parsing

-Browser fingerprint customization (e.g., `user-agent`, disabling `headless flags`)

-Randomized mouse movements, scrolls, and delays to simulate human interaction

We also implemented robust logging to track scraper behavior and detect blocking events early.

###🛠️ Technologies Used

-Python

-Selenium (undetected-chromedriver)

-BeautifulSoup4

-Python’s built-in logging module

###⚔️ Challenge & How We Solved It

`Challenge`: Target websites actively blocked bots via automation detection and fingerprinting.

`Solution`:

-Used `selenium-stealth` and `undetected-chromedriver` to mask bot behavior

-Removed JavaScript flags like `navigator.webdriver`

-Employed real Chrome `user-agent` strings

-Evaded detection by avoiding patterns like `$cdc_` typically used by headless automation tools.
