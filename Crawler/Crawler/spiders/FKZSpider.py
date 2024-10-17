import os
from dotenv import load_dotenv
import scrapy
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import dateparser
from Crawler.items import MarketItem
from services.translator import Translator

class MarketSpiderFKZ(scrapy.Spider):
    """
    A Scrapy spider for scraping and translating news articles from finance.kz that are published on the current date. 
    It uses Selenium for dynamic content loading and a custom translation service for translating content.
    """
    name = "FKZSpider"
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'Crawler.middlewares.SeleniumMiddleware': 800,
        }
    }

    def __init__(self, *args, **kwargs):
        load_dotenv()
        super(MarketSpiderFKZ, self).__init__(*args, **kwargs)
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logging.error("OPENAI_API_KEY not found in environment variables")
            raise EnvironmentError("OPENAI_API_KEY not found in environment variables")
        
        self.translator = Translator(api_key=api_key)
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_service = Service(self.get_chromedriver_path())
        self.driver = webdriver.Chrome(service=chrome_service, options=chrome_options)

    def get_chromedriver_path(self):
        """
        Determines the correct path for chromedriver based on the operating system.
        """

        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        if os.name == 'nt': 
            return os.path.join(project_root, 'chromedriver.exe')
        else:
            return '/path/to/your/chromedriver'

    def start_requests(self):
        urls = ['https://finance.kz/news']
        for url in urls:
            yield scrapy.Request(url, callback=self.parse_articles, meta={'use_selenium': True})

    def parse_articles(self, response):
        """
        Processes each article element, extracting the URL and scheduling a parse callback.
        """
        self.driver.get(response.url)
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div.record-item-block'))
            )
            articles = self.driver.find_elements(By.CSS_SELECTOR, 'div.record-item-block')
            for article in articles:
                link = article.find_element(By.TAG_NAME, 'a')
                href = link.get_attribute('href')
                date_span = article.find_element(By.CSS_SELECTOR, 'span.record-item-date').text.strip()
                article_date = dateparser.parse(date_span, languages=['ru']).date()
                if href and article_date == datetime.now().date():
                    yield scrapy.Request(href, callback=self.parse_article_content, meta={'use_selenium': True})
        except TimeoutException:
            logging.error(f'Timeout while loading {response.url}')
        except WebDriverException as e:
            logging.error(f"WebDriver exception on {response.url}: {e}")
    
    def parse_article_content(self, response):
        """
        Parses individual news articles to extract relevant information.
        """

        self.driver.get(response.url)
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.record-page-date'))
            )
            date_str_element = self.driver.find_element(By.CSS_SELECTOR, 'div.record-page-date').text.strip()
            date_obj = dateparser.parse(date_str_element)
            if date_obj.date() == datetime.now().date():
                news_item = MarketItem()
                news_item['date'] = date_obj.strftime('%Y-%m-%d')

                header = self.driver.find_element(By.CSS_SELECTOR, 'h1').text
                translated_header = self.translator.translate_text(header)
                news_item['header'] = translated_header if translated_header else "Empty"
                
                sub_header = self.driver.find_element(By.CSS_SELECTOR, 'h3').text
                translated_sub_header = self.translator.translate_text(sub_header)
                news_item['sub_header'] = translated_sub_header if translated_sub_header else "Empty"
                
                news_item['label'] = "Central Asia"
                
                paragraphs = self.driver.find_elements(By.CSS_SELECTOR, 'div.record-page-body > p')
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
