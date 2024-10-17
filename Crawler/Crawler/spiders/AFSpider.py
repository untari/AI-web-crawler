import scrapy
import json
from scrapy.exceptions import DropItem
from datetime import datetime
from w3lib.html import remove_tags
import logging
from Crawler.items import MarketItem

class MarketSpiderAFS(scrapy.Spider):
    """
    A scrapy spider for scraping news articles from asiafinancial.com that are published on the current date.
    """
    name = "AFSpider"
    allowed_domains = ["asiafinancial.com"]
    start_urls = ["https://www.asiafinancial.com/insights/"]

    def parse(self, response):
        """
        Parses the main page to extract links to individual news articles.
        """
        current_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S') 
        try:
            HotNews = response.css('div.col-md-8 div.tt-post.has-thumbnail.type-6.clearfix.post-430.post.type-post.status-publish.format-standard.has-post-thumbnail.hentry.category-business.category-culture.tag-all.tag-health.tag-politics')

            for Hot in HotNews:
                relative_url = Hot.css('div.tt-post-info a.tt-post-title.c-h5 ::attr(href)').get()
                if relative_url:
                    yield response.follow(relative_url, callback=self.parse_news_content)
        except Exception as e:
            logging.error('Error processing %s: %s', current_datetime, response.url, str(e))

    def parse_news_content(self, response):
        """
        Parses individual news articles to extract relevant information.
        """
        content = response.css('div.col-md-8')
        news_item = MarketItem()

        date_text = content.css('div.tt-post.type-3 div.tt-post-label span.tt-post-date::text').get()
        try:
            if date_text:
                date_obj = datetime.strptime(date_text, '%B %d, %Y')
                # if date_obj.date() == datetime.now().date():
                news_item['date'] = date_obj.strftime('%Y-%m-%d')
                news_item['label'] = content.css('div.tt-post-label.tt-post-label-new span.tt-post-label-new.tt-post-cat a::text').get()
                news_item['header'] = content.css('div.reports-big-head h1.reports-big-head::text').get()
                news_item['sub_header'] = content.css('div.col.post-excerpt p::text').get()
                news_item['img'] = content.css('div.story-big-img img::attr(src)').get()
                news_item['img_caption'] = content.css('div.story-big-img figcaption::text').get()

                raw_content = content.css('div.content p,strong a, h2, li::text').getall()
                news_item['content'] = ' '.join(raw_content).replace('\xa0', '').replace('\n', '').strip()

                yield news_item
                # else:
                #     self.logger.info(f"Skipping article, not from today: {date_text}")
            else:
                raise ValueError("Missing date in article")
        except ValueError as e:
            news_item['content'] = "Empty"
            logging.error('Error processing article: %s', response.url)
            raise DropItem(f"Missing date in article: {response.url}")
        
