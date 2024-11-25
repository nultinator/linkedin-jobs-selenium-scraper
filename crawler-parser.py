import os
import csv
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
import logging
from urllib.parse import urlencode
import concurrent.futures
from dataclasses import dataclass, field, fields, asdict

API_KEY = ""

with open("config.json", "r") as config_file:
    config = json.load(config_file)
    API_KEY = config["api_key"]

options = webdriver.ChromeOptions()
options.add_argument("--headless")


## Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def scrape_search_results(keyword, location, locality, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    formatted_locality = locality.replace(" ", "+")
    url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={formatted_keyword}&location={formatted_locality}"
    tries = 0
    success = False

    
    while tries <= retries and not success:

        driver = webdriver.Chrome(options=options)

        try:
            driver.get(url)
                        
            div_cards = driver.find_elements(By.CSS_SELECTOR, "div[class='base-search-card__info']")

            if not div_cards:
                driver.save_screenshot("debug.png")
                raise Exception("Page did not load correctly, please check debug.png")

            for div_card in div_cards:
                company_name = div_card.find_element(By.CSS_SELECTOR, "h4[class='base-search-card__subtitle']").text
                print("company name", company_name)
                job_title = div_card.find_element(By.CSS_SELECTOR, "h3[class='base-search-card__title']").text
                parent = div_card.find_element(By.XPATH, "..")
                link = parent.find_element(By.CSS_SELECTOR, "a")
                job_link = link.get_attribute("href")
                location = div_card.find_element(By.CSS_SELECTOR, "span[class='job-search-card__location']").text
                
                search_data = {
                    "name": company_name,
                    "job_title": job_title,
                    "url": job_link,
                    "location": location
                }          

                print(search_data)
            logger.info(f"Successfully parsed data from: {url}")
            success = True
        
                    
        except Exception as e:
            logger.error(f"An error occurred while processing page {url}: {e}")
            logger.info(f"Retrying request for page: {url}, retries left {retries-tries}")
            tries+=1

        finally:
            driver.quit()

    if not success:
        raise Exception(f"Max Retries exceeded: {retries}")


if __name__ == "__main__":

    MAX_RETRIES = 3
    MAX_THREADS = 5
    PAGES = 1
    LOCATION = "us"
    LOCALITY = "United States"

    logger.info(f"Crawl starting...")

    ## INPUT ---> List of keywords to scrape
    keyword_list = ["software engineer"]
    aggregate_files = []

    ## Job Processes
    for keyword in keyword_list:
        filename = keyword.replace(" ", "-")

        scrape_search_results(keyword, LOCATION, LOCALITY, retries=MAX_RETRIES)
        
    logger.info(f"Crawl complete.")