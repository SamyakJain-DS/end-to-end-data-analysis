# importing libraries and packages
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
import pickle
import logging
import time
import random

# logging setup
logging.basicConfig(
    filename='app.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


def delay(min_sec=3, max_sec=5):
    '''
    Adds a random delay of minimum min_sec seconds and maximum max_sec seconds.
    min_sec should be a positive number less tahn max_sec.
    '''

    time.sleep(random.uniform(min_sec, max_sec))


def scrape(categories, retry=False):
    '''
    This function scrapes all devices of the categories provided in the "categories" tuple.
    The input should be a TUPLE of categories or category.
    Scraping is done on the basis of brands to bypass a maximum limit of page length imposed by the website.
    Failed cases are logged and returned as a separate object.
    This function can also be used to retry scraping for the failed cases.
    Set the retry to True while trying to scrape the failed cases.
    '''

    categories_htmls = dict()
    failed_cases = dict()

    if not retry:
        categories_iter = categories
    else:
        categories_iter = categories.keys()

    for category in categories_iter:
        failed_cases[category] = []  # making the code error proof
        categories_htmls[category] = dict()
        htmls = dict()  # will carry all the htmls for different brands

        if not retry:
            driver = uc.Chrome(headless=False)  # setting up an undetected chrome
            try:
                driver.get(f"https://www.smartprix.com/{category}")
                brands_div = driver.find_elements(by=By.CLASS_NAME, value='sm-filters-list')[1].text.lower().replace(
                    " ", "_")
                brands = brands_div.split('\n')[0::2]  # got the brands for the given category
                logging.info(f"Extracted The Brands List For {category}")
            except Exception as e:
                logging.error(f"Failed To Retrieve Brands List : {e}")
                logging.warning(f"Added {category} to Failed Cases.")
                failed_cases[category] = 'all, retry'
                continue  # if we fail to retrieve brands, move to the next category and retry for this category later
            driver.quit()
        else:
            brands = categories[category]

        for brand in brands:  # now for each brand page
            flag = False
            brand_url = f"https://www.smartprix.com/{category}/{brand}-brand"
            brand_driver = uc.Chrome(headless=False)

            try:
                brand_driver.get(brand_url)
                logging.info(f"Loaded The {brand} Page ({category})")
                old_height = brand_driver.execute_script('return document.body.scrollHeight')

                while True:

                    delay()
                    load_more = brand_driver.find_elements(by=By.CLASS_NAME, value='sm-load-more')
                    if load_more:
                        brand_driver.execute_script(
                            "arguments[0].scrollIntoView({ behavior: 'smooth', block: 'center' });", load_more[0])
                        delay(1, 1)
                        load_more[0].click()  # click the Load More button
                    delay()
                    new_height = brand_driver.execute_script('return document.body.scrollHeight')

                    if new_height == old_height:
                        total_results = int(
                            brand_driver.find_element(by=By.CLASS_NAME, value='pg-prf-head').text.split(" ")[0])
                        fetched_results = len(brand_driver.find_elements(by=By.CLASS_NAME, value='sm-product-actions'))
                        logging.warning(
                            f"Fetched Results For {brand} ({category}) : {fetched_results}/{total_results} ")

                        if total_results != min(fetched_results, 1020):
                            failed_cases[category].append(brand)
                            logging.warning(f"Added {brand} ({category}) To Failed Cases.")
                            flag = True
                        break  # breaks the loop when there is nothing more to load
                    else:
                        old_height = new_height
                if flag:
                    brand_driver.quit()
                    continue

                htmls[brand] = brand_driver.page_source  # dictionary {brand: html page}
                logging.info(f"Fetched {brand} ({category}) Successfully")
                brand_driver.quit()

            except Exception as e:
                logging.error(f"Failed To Fetch The {brand} HTML Page ({category})")
                logging.warning(f"Added {brand} ({category}) To Failed Cases.")
                failed_cases[category].append(brand)  # if we fail to get the page, add it to failed_cases and move on
                brand_driver.quit()
                continue

        categories_htmls[category] = htmls
        logging.info(f"Fetched {category} Successfully")

    return categories_htmls, failed_cases


def exhaustive_scrape(categories_htmls, failed_cases, max_attempts=5):
    '''
    This function's purpose is to scrape in a loop, until either:
    1. we have exhausted the entire failed_cases dictionary, and have no more failed_cases
    2. loop has already been run "max_attempt" times. This is to prevent infinite loops in case of unforeseen problems.
    '''

    for attempt in range(max_attempts):
        total_failed_cases = 0
        for category in failed_cases.keys():  # check if the list for each category's failed cases is empty or not
            total_failed_cases += len(failed_cases[category])

        if total_failed_cases == 0:
            break  # break if every list is empty

        new_htmls, failed_cases = scrape(failed_cases, retry=True)
        for category in new_htmls.keys():  # merge this new htmls with htmls
            for brand in new_htmls[category].keys():
                categories_htmls[brand] = new_htmls[category][brand]

    return categories_htmls

categories = ("mobiles", "laptops", "tablets",)

htmls, failed = scrape(categories)
final_htmls = exhaustive_scrape(htmls, failed)

with open("final_htmls.pkl", "wb") as file:
    pickle.dump(final_htmls, file)