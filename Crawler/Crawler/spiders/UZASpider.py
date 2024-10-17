import scrapy
import os
from datetime import datetime
import logging
from Crawler.items import MarketItem
from services.translator import Translator
import dateparser
from dotenv import load_dotenv
from selenium.common.exceptions import TimeoutException, WebDriverException


class MarketSpiderUZA(scrapy.Spider):
    name = "UZASpider"
    allowed_domains = ["uza.uz"]
    start_urls = ["https://uza.uz/"]

    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.redirect.MetaRefreshMiddleware': None,
        },
    }

    def __init__(self, *args, **kwargs):
        """
        Initializes the spider with environment variables and a translator service.
        """
        load_dotenv()
        super(MarketSpiderUZA, self).__init__(*args, **kwargs)
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise EnvironmentError("OPENAI_API_KEY not found in environment variables")
        self.translator = Translator(api_key=api_key)

    def parse(self, response):
        """
        Identifies news articles on the page and queues them for content parsing.
        """
        top_news_links = response.css('div.last-news-list a.small-news__title::attr(href)').getall()
        if not top_news_links:
            logging.error('No news links found on the page: {}'.format(response.url))
        for url in top_news_links:
            full_url = response.urljoin(url)
            yield response.follow(full_url, self.parse_news_content)

    def parse_news_content(self, response):
        date_text = response.css('div.news-top-head__date::text').get()
        if not date_text:
            logging.error('Date not found for article: {}'.format(response.url))
            return

        parsed_date = dateparser.parse(date_text.strip(), languages=['ru'])
        if not parsed_date or parsed_date.date() != datetime.now().date():
            logging.info(f"Skipping article, not from today: {date_text}")
            return

        try:
            item = self.extract_article_info(response, parsed_date)
            yield item
        except Exception as e:
            logging.error(f"Error extracting article content: {e}")

    def extract_article_info(self, response, parsed_date):
        item = MarketItem()
        try:
            item['date'] = parsed_date.strftime('%Y-%m-%d')
            item['header'] = self.translate_and_extract_text(response, 'div.news-top-head__title::text')
            content = self.translate_and_extract_text(response, 'div.content-block p::text', join=True)
            item['content'] = content if content not in [None, ""] else "Empty"
            item['label'] = "Business"
            item['sub_header'] = "Empty" 
            return item
        except TimeoutException as e:
            item['content'] = "Empty"
            logging.error(f"Timeout waiting for article content to load on {response.url}: {e}")
        except WebDriverException as e:
            item['content'] = "Empty"
            logging.error(f"WebDriver exception while parsing article content on {response.url}: {e}")
        except Exception as e:
            item['content'] = "Empty"
            logging.error(f"Unexpected error while parsing article content on {response.url}: {e}")
        

    def translate_and_extract_text(self, response, css_selector, join=False):
        texts = response.css(css_selector).getall()
        if join:
            text = ' '.join(texts).strip()
        else:
            text = texts[0].strip() if texts else None
        return self.translator.translate_text(text) if text else None
