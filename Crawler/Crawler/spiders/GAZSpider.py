import os
import scrapy
import logging
from datetime import datetime
import dateparser
from Crawler.items import MarketItem
from services.translator import Translator
from dotenv import load_dotenv
from selenium.common.exceptions import TimeoutException, WebDriverException


class MarketSpiderGAZ(scrapy.Spider):
    """
    Spider for scraping today's economy news from gazeta.uz and translating it.
    """
    name = "GAZSpider"
    allowed_domains = ["gazeta.uz"]
    start_urls = ["https://www.gazeta.uz/uz/economy?page=1"]

    def __init__(self, *args, **kwargs):
        load_dotenv()
        super().__init__(*args, **kwargs)
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logging.error("OPENAI_API_KEY not found in environment variables")
            raise EnvironmentError("OPENAI_API_KEY not found in environment variables")
        self.translator = Translator(api_key=api_key)

    def parse(self, response):
        """
        Processes each article element, extracting the URL and scheduling a parse callback.
        """
        econNews = response.css('div.nblock')
        if not econNews:
            logging.error(f'No business news content found on: {response.url}')
            return
        for article in econNews:
            relative_url = article.css('a::attr(href)').get()
            if relative_url:
                full_url = response.urljoin(relative_url)
                yield response.follow(full_url, self.parse_news_content)
            else:
                logging.warning('Missing URL in article listing: {}'.format(response.url))

    def parse_news_content(self, response):
        """
        Parses individual news articles to extract relevant information.
        """
        try:
            date_text = response.css('div.articleDateTime::text').extract_first(default='').strip()
            parsed_date = self.handle_date(date_text)
            if not parsed_date or parsed_date.date() != datetime.now().date():
                logging.info(f"Skipping article, not from today: {date_text}")
                return
            yield self.extract_article_info(response, parsed_date)
        except Exception as e:
            logging.error(f"Error extracting article content from {response.url}: {e}")

    def extract_article_info(self, response, parsed_date):
        """
        Extracts details from the article response and prepares them for translation.
        """
        item = MarketItem()
        try:
            item['date'] = parsed_date.strftime('%Y-%m-%d')
            item['header'] = self.translate_text(response.css('h1::text').get())
            item['sub_header'] = self.translate_text(response.css('h4::text').get())
            item['img_caption'] = self.translate_text(response.css('p.articlePicDesc::text').get())
            item['label'] = self.translate_text(response.css('div.articleDateTime a span::text').get())
            item['content'] = self.translate_text(' '.join(response.css('div.articleContent.type-news p::text').extract()))
            item['img'] = response.css('img.lazy.articleBigPic::attr(data-src)').get()
            return item
        except WebDriverException as e:
            item['content'] = "Empty"
            logging.error(f"WebDriver exception while parsing article content on {response.url}: {e}")
        except Exception as e:
            item['content'] = "Empty"
            logging.error(f"Unexpected error while parsing article content on {response.url}: {e}")
            


    def handle_date(self, date_text):
        """
        Parses the publication date from a string, handling relative dates like 'Today'.
        """
        try:
            if 'Бугун' in date_text:
                return datetime.now()
            return dateparser.parse(date_text)
        except Exception as e:
            logging.error(f"Error parsing date: {date_text}, Error: {e}")
            return None

    def translate_text(self, text):
        """
        Translates text using the configured translation service.
        """
        try:
            if text:
                text = text.strip()
                return self.translator.translate_text(text)
        except Exception as e:
            logging.error(f"Translation error for text: {e}")
        return "Empty"
