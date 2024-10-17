import scrapy
from scrapy.exceptions import DropItem
from datetime import datetime
from w3lib.html import remove_tags
import logging
from Crawler.items import MarketItem

class MarketSpiderAST(scrapy.Spider):
    """
    A scrapy spider for scraping business news articles from astanatimes.com that are published on the current date.
    """
    name = "ASTSpider"
    allowed_domains = ["astanatimes.com"]
    start_urls = ["https://astanatimes.com/category/business/"]

    custom_settings = {
        'FEEDS': {
            'market_data.json': {'format': 'json', 'overwrite': True},
        }
    }

    def parse(self, response):
        """
        Parses the main page to extract links to individual news articles.
        """
        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            HotNews = response.css('div.five.columns.border-left div.row.featuredlist')

            logging.info(f"Number of URLs found: {len(HotNews)}")
            for Hot in HotNews:
                relative_url = Hot.css('h4 a::attr(href)').get()
                if relative_url:
                    yield response.follow(relative_url, callback=self.parse_news_content)
        
        except Exception as e:
            logging.error('Error processing %s: %s', current_datetime, response.url, str(e))

    def parse_news_content(self, response):
        """
        Parses individual news articles to extract relevant information.
        """
        news_item = MarketItem()

        date_text = response.css('p.byline::text').getall()
        try:
            if date_text:
                date = [text.strip() for text in date_text if text.strip()]
                date_string = date[-1]
                try:
                    date_obj = datetime.strptime(date_string, '%d %B %Y')
                except ValueError:
                    logging.error('Date format error for article: %s', response.url)
                    raise DropItem(f"Invalid date format in article: {response.url}")
            else:
                logging.error('No date found for article: %s', response.url)
                raise DropItem("Missing date in article")

            if date_obj.date() == datetime.now().date():
                news_item['date'] = date_obj.strftime('%Y-%m-%d')
                news_item['label'] = "Business"
                news_item['header'] = response.css('div.eight.columns h1::text').get()
                news_item['sub_header'] = "Empty" 
                news_item['img'] = response.css('div.post div.wp-caption.aligncenter img::attr(src)').get()
                news_item['img_caption'] = response.css('div.post p.wp-caption-text::text').get()
                news_item['content'] = ' '.join(response.css('div.post p span::text').getall())

                yield news_item
            else:
                logging.info(f"Skipping article, not from today: {date_string}")
        except ValueError as e:
            news_item['content'] = "Empty"
            logging.error('Error processing article: %s', response.url)
            raise DropItem(f"Missing date in article: {response.url}")
    

