import os
import scrapy
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from Crawler.items import MarketItem
from datetime import datetime
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException


class MarketSpiderCAT(scrapy.Spider):
    """
    A Scrapy spider integrated with Selenium for scraping dynamically loaded news articles
    from centralasia.tech, focusing on today's articles related to Central Asia.
    """
    name = "CATSpider"
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'Crawler.middlewares.SeleniumMiddleware': 800
        }
    }

    def __init__(self):
        """
        Initializes the spider with environment variables and a translator service.
        """
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Enables headless mode for Chrome

        # Set the path to chromedriver dynamically based on the operating system
        if os.name == 'nt':
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
            path_to_chromedriver = os.path.join(project_root, 'chromedriver.exe')  # Path for Windows
            chrome_service = Service(path_to_chromedriver)
            self.driver = webdriver.Chrome(service=chrome_service, options=chrome_options)
        else:
            self.driver = webdriver.Chrome(options=chrome_options)  # Assumes chromedriver is in PATH for non-Windows

    def start_requests(self):
        urls = ['https://www.centralasia.tech/media']
        for url in urls:
            yield scrapy.Request(url, callback=self.parse, meta={'use_selenium': True})

    def parse(self, response):
        """
        Processes each article element, extracting the URL and scheduling a parse callback.
        """
        try:
            self.driver.get(response.url)

            # Ensure that the necessary elements are loaded
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div[class^="max-w-\\[650px\\]"] a'))
            )

            elements = self.driver.find_elements(By.CSS_SELECTOR, 'div[class^="max-w-\\[650px\\]"] a')
            relative_urls = [element.get_attribute('href') for element in elements]

            for relative_url in relative_urls:
                yield scrapy.Request(relative_url, callback=self.parse_news_content, meta={'use_selenium': True})
        except TimeoutException as e:
            logging.error(f"Timeout waiting for page elements on {response.url}: {e}")
        except WebDriverException as e:
            logging.error(f"WebDriver error on {response.url}: {e}")

    def parse_news_content(self, response):
        """
        Parses individual news articles to extract relevant information.
        """
        try:
            self.driver.get(response.url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'h4.font-medium'))
            )

            date_text = self.driver.find_element(By.XPATH, "//h4[contains(@class, 'text-end')]").text.strip()
            date_obj = datetime.strptime(date_text, '%Y-%m-%d')

            if date_obj.date() == datetime.now().date():
                news_item = MarketItem()
                header = self.driver.find_element(By.XPATH, "//h4[contains(@class, 'font-medium')]").text
                news_item['header'] = header if header else None
                news_item['date'] = date_obj.strftime('%Y-%m-%d')
                news_item['label'] = "Central Asia"
                news_item['sub_header'] = "Empty"

                paragraphs = self.driver.find_elements(By.CSS_SELECTOR, 'div.md\\:px-14 > p')
                content = ' '.join(p.get_attribute('textContent').strip() for p in paragraphs if p.get_attribute('textContent').strip())
                news_item['content'] = content

                yield news_item
            else:
                logging.info('Skipping article, not from today: %s', response.url)
        except WebDriverException as e:
            news_item['content'] = "Empty"
            logging.error(f"WebDriver exception while parsing article content on {response.url}: {e}")
        except Exception as e:
            news_item['content'] = "Empty"
            logging.error(f"Unexpected error while parsing article content on {response.url}: {e}")
        

    def closed(self, reason):
        """
        Ensures the Selenium webdriver is properly closed when the spider is closed.
        """
        self.driver.quit()
