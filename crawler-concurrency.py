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



@dataclass
class SearchData:
    name: str = ""
    job_title: str = ""
    url: str = ""
    location: str = ""

    def __post_init__(self):
        self.check_string_fields()
        
    def check_string_fields(self):
        for field in fields(self):
            # Check string fields
            if isinstance(getattr(self, field.name), str):
                # If empty set default text
                if getattr(self, field.name) == "":
                    setattr(self, field.name, f"No {field.name}")
                    continue
                # Strip any trailing spaces, etc.
                value = getattr(self, field.name)
                setattr(self, field.name, value.strip())


class DataPipeline:
    
    def __init__(self, csv_filename="", storage_queue_limit=50):
        self.names_seen = []
        self.storage_queue = []
        self.storage_queue_limit = storage_queue_limit
        self.csv_filename = csv_filename
        self.csv_file_open = False
    
    def save_to_csv(self):
        self.csv_file_open = True
        data_to_save = []
        data_to_save.extend(self.storage_queue)
        self.storage_queue.clear()
        if not data_to_save:
            return

        keys = [field.name for field in fields(data_to_save[0])]
        file_exists = os.path.isfile(self.csv_filename) and os.path.getsize(self.csv_filename) > 0
        with open(self.csv_filename, mode="a", newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys)

            if not file_exists:
                writer.writeheader()

            for item in data_to_save:
                writer.writerow(asdict(item))

        self.csv_file_open = False
                    
    def is_duplicate(self, input_data):
        if input_data.name in self.names_seen:
            logger.warning(f"Duplicate item found: {input_data.name}. Item dropped.")
            return True
        self.names_seen.append(input_data.name)
        return False
            
    def add_data(self, scraped_data):
        if self.is_duplicate(scraped_data) == False:
            self.storage_queue.append(scraped_data)
            if len(self.storage_queue) >= self.storage_queue_limit and self.csv_file_open == False:
                self.save_to_csv()
                       
    def close_pipeline(self):
        if self.csv_file_open:
            time.sleep(3)
        if len(self.storage_queue) > 0:
            self.save_to_csv()



def scrape_search_results(keyword, location, locality, page_number, data_pipeline=None, retries=3):
    formatted_keyword = keyword.replace(" ", "+")
    formatted_locality = locality.replace(" ", "+")
    url = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords={formatted_keyword}&location={formatted_locality}&original_referer=&start={page_number*10}"
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
                
                search_data = SearchData(
                    name=company_name,
                    job_title=job_title,
                    url=job_link,
                    location=location
                )           

                data_pipeline.add_data(search_data)
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




def start_scrape(keyword, pages, location, locality, data_pipeline=None, max_threads=5, retries=3):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        executor.map(
            scrape_search_results,
            [keyword] * pages,
            [location] * pages,
            [locality] * pages,
            range(pages),
            [data_pipeline] * pages,
            [retries] * pages
        )


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

        crawl_pipeline = DataPipeline(csv_filename=f"{filename}.csv")
        start_scrape(keyword, PAGES, LOCATION, LOCALITY, data_pipeline=crawl_pipeline, max_threads=MAX_THREADS, retries=MAX_RETRIES)
        crawl_pipeline.close_pipeline()
        aggregate_files.append(f"{filename}.csv")
    logger.info(f"Crawl complete.")