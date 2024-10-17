import scrapy
import os
from datetime import datetime
import logging
from Crawler.items import MarketItem
from services.translator import Translator
import dateparser
from dotenv import load_dotenv
from selenium.common.exceptions import TimeoutException, WebDriverException


class MarketSpiderSPT(scrapy.Spider):
    name = "SPTSpider"
    allowed_domains = ["spot.uz"]
    start_urls = ["https://www.spot.uz/ru/business/"]

    def __init__(self, *args, **kwargs):
        load_dotenv()
        super(MarketSpiderSPT, self).__init__(*args, **kwargs)
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise EnvironmentError("OPENAI_API_KEY not found in environment variables")
        self.translator = Translator(api_key=api_key)

    def parse(self, response):
        """
        Identifies business news articles on the page and queues them for content parsing.
        """
        businessNews = response.css('div.contentBox')
        if not businessNews:
            self.logger.error(f'No business news content found on: {response.url}')
        for article in businessNews:
            relative_url = article.css('h2.itemTitle a::attr(href)').get()
            if relative_url:
                full_url = response.urljoin(relative_url)
                yield response.follow(full_url, self.parse_news_content)
            else:
                self.logger.warning('Missing URL in article listing.')

    def parse_news_content(self, response):
        """
        Extracts content from an individual news article page and processes it.
        """
        date_string = response.css('div.itemData span::text').get().strip()
        parsed_date = self.parse_date(date_string)

        if not parsed_date:
            self.logger.error(f'Error parsing date for article: {response.url}')
            return

        if parsed_date.date() != datetime.now().date():
            self.logger.info(f"Skipping article, not from today: {date_string}")
            return

        try:
            news_item = self.extract_article_info(response, parsed_date)
            yield news_item
        except Exception as e:
            self.logger.error(f"Error extracting article content: {e}, URL: {response.url}")

    def extract_article_info(self, response, parsed_date):
        """
        Extracts details from the article response and prepares them for translation.
        """
        item = MarketItem()
        try:
            item['date'] = parsed_date.strftime('%Y-%m-%d')
            item['label'] = self.translate_text(response.css('div.itemData a span::text').get())
            item['header'] = self.translate_text(response.css('h1::text').get())
            item['sub_header'] = self.translate_text(response.css('div.articleContent p::text').get())
            item['img'] = response.css('div.articleContent a::attr(href)').get()
            item['img_caption'] = self.translate_text(response.css('div.postPicDesc::text').get())
            item['content'] = self.translate_text(' '.join(response.css('div.js-mediator-article.article-text p::text').extract()))
            return item
        except WebDriverException as e:
            logging.error(f"WebDriver exception while parsing article content on {response.url}: {e}")
            item['content'] = "Empty"
        except Exception as e:
            item['content'] = "Empty"
            logging.error(f"Unexpected error while parsing article content on {response.url}: {e}")
        

    def parse_date(self, date_string):
        """
        Parses the publication date from a string, handling relative dates like 'Today'.
        """
        if "Сегодня" in date_string:
            return datetime.now()
        return dateparser.parse(date_string)

    def translate_text(self, text):
        """
        Translates text using the configured translation service.
        """
        if text:
            text = text.strip()
            return self.translator.translate_text(text)
        return "Empty"
