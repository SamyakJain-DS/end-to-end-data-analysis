### Data Gathering
We set up a dedicated Python environment (venv/conda) and Git for version control to ensure a reproducible workflow. We collected electronic device data by scraping retail and review websites – many of which use dynamic JavaScript and anti-bot defenses. To handle this, we automated Chrome with undetected-chromedriver and Selenium, combined with BeautifulSoup for parsing. We customized the browser fingerprint (e.g. user-agent, headless flags) and introduced randomized delays and mouse/keyboard emulation to mimic human behavior. This stealth scraping approach helped us bypass advanced anti-bot measures like headless detection and fingerprinting
kameleo.io
zenrows.com
. We also implemented robust logging of requests and responses to monitor our scraper’s progress and catch any blocks early.
Technologies: Python, Selenium (undetected-chromedriver), BeautifulSoup4, logging.
Challenge & Solution: Target sites actively detect scrapers. We solved this by using the Selenium Stealth plugin and undetected-chromedriver to mask automation signals
zenrows.com
kameleo.io
. For example, we removed the navigator.webdriver flag and used real Chrome user agents to avoid the common $cdc_ signature check
zenrows.com
.
