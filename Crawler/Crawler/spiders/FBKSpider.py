import os
from dotenv import load_dotenv
import scrapy
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import logging
import dateparser
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from services.translator import Translator
from Crawler.items import MarketItem
from selenium.common.exceptions import TimeoutException, WebDriverException


class MarketSpiderFBK(scrapy.Spider):
    """
    A Scrapy spider integrated with Selenium and a translation service for scraping
    and translating news articles from forbes.kz, focusing only on articles published today.
    """
    name = "FBKSpider"
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'Crawler.middlewares.SeleniumMiddleware': 800
        }
    }

    def __init__(self, *args, **kwargs):
        """
        Initializes the spider with environment variables and a translator service.
        """
        load_dotenv()  # Load environment variables from .env file
        super(MarketSpiderFBK, self).__init__(*args, **kwargs)
        api_key = os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logging.error("OPENAI_API_KEY not found in environment variables")
            raise EnvironmentError("OPENAI_API_KEY not found in environment variables")
        
        self.translator = Translator(api_key=api_key)  # Initialize translator with API key
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Enables headless mode for Chrome
        chrome_service = Service(self.get_chromedriver_path())
        self.driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

    def get_chromedriver_path(self):
        """
        Determines the correct path for chromedriver based on the operating system.
        """
        if os.name == 'nt':
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
            return os.path.join(project_root, 'chromedriver.exe')  # Path for Windows
        else:
            return '/path/to/your/chromedriver'  # Adjust this path for non-Windows OS

    def start_requests(self):
        urls = ['https://forbes.kz/news']
        for url in urls:
            yield scrapy.Request(url, callback=self.parse, meta={'use_selenium': True})

    def parse(self, response):
        """
        Processes each article element, extracting the URL and scheduling a parse callback.
        """
        try:
            self.driver.get(response.url)
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a.news__mini-info'))
            )

            elements = self.driver.find_elements(By.CSS_SELECTOR, 'a.news__mini-info')
            relative_urls = [element.get_attribute('href') for element in elements][:2]  # Limit to 2 for demonstration

            for relative_url in relative_urls:
                yield scrapy.Request(relative_url, callback=self.parse_news_content, meta={'use_selenium': True})
        except TimeoutException:
            logging.error(f'Timeout while loading {response.url}')
        except WebDriverException as e:
            logging.error(f"WebDriver exception on {response.url}: {e}")

    def parse_news_content(self, response):
        """
        Parses individual news articles to extract relevant information.
        """
        self.driver.get(response.url)
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'article[class*="article-id"]'))
            )

            date_text = self.driver.find_element(By.CSS_SELECTOR, 'div.article__date span').text.strip()
            date_obj = dateparser.parse(date_text)

            if date_obj.date() == datetime.now().date():
                news_item = MarketItem()
                header = self.driver.find_element(By.CSS_SELECTOR, 'article[class*="article-id"] h1').text

                translated_header = self.translator.translate_text(header)
                news_item['header'] = translated_header if translated_header else None
                news_item['date'] = date_obj.strftime('%Y-%m-%d')
                news_item['label'] = "Central Asia"
                news_item['sub_header'] = "Empty" 

                paragraphs = self.driver.find_elements(By.CSS_SELECTOR, 'article[class*="inner-news"] p')
                content = ' '.join(p.text.strip() for p in paragraphs)

                translated_content = self.translator.translate_text(content)
                news_item['content'] = translated_content if translated_content else None

                yield news_item
            else:
                logging.info(f"Skipping article from {date_obj.strftime('%Y-%m-%d')}, not today's date.")
        except WebDriverException as e:
            news_item['content'] = "Empty"
            logging.error(f"WebDriver exception while parsing article content on {response.url}: {e}")
        except Exception as e:
            news_item['content'] = "Empty"
            logging.error(f"Unexpected error while parsing article content on {response.url}: {e}")
            


    def closed(self, reason):
        """
        Ensures the Selenium webdriver is properly closed when the spider is closed to prevent resource leaks.
        """
        self.driver.quit()
