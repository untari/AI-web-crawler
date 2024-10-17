import scrapy
import re
from datetime import datetime
from Crawler.items import MarketItem

class UZReportSpider(scrapy.Spider):
    name = "UZReportSpider"
    allowed_domains = ["uzreport.news"]
    start_urls = ["https://www.uzreport.news"]

    def parse(self, response):
        articles = response.css('div.search-content.hidden-xs h3 a::attr(href)').extract()
        for article_url in articles:
            yield response.follow(article_url, self.parse_news_content)

    def parse_news_content(self, response):
        item = MarketItem()

        date_string = ''.join(response.css('li.time a::text').extract()).strip()
        date_string = self.translate_date_to_english(date_string)
        date_string = self.clean_date_string(date_string)

        item['date'] = self.parse_date(date_string)
        item['label'] = response.css('div.center_panel li.rubric a::text').get().strip()
        item['header'] = response.css('div.center_panel h1::text').get().strip()
        item['sub_header'] = '-'  # Placeholder if there's no sub-header
        item['img'] = response.css('div.center_panel img.news-page_img::attr(src)').get()
        item['img_caption'] = '-'  # Placeholder if there's no image caption
        item['content'] = ' '.join(response.css('div.center_panel p::text').extract()).strip()
        
        yield item

    def translate_date_to_english(self, date_str):
        translations = {
            'yanvar': 'January',
            'fevral': 'February',
            'mart': 'March',
            'aprel': 'April',
            'may': 'May',
            'iyun': 'June',
            'iyul': 'July',
            'avgust': 'August',
            'sentyabr': 'September',
            'oktyabr': 'October',
            'noyabr': 'November',
            'dekabr': 'December'
        }
        for uz, en in translations.items():
            date_str = date_str.replace(uz, en)
        return date_str

    def clean_date_string(self, date_str):
        # Find the year using regex and keep everything up to that point
        year_match = re.search(r'\d{4}', date_str)
        if year_match:
            year_index = year_match.end()
            date_str = date_str[:year_index]
        return date_str

    def parse_date(self, date_str):
        try:
            # Assuming the cleaned date string is now in a 'day Month year' format
            date_obj = datetime.strptime(date_str, '%H:%M, %d %B %Y')
            return date_obj.strftime('%Y-%m-%d')
        except ValueError as e:
            self.logger.error(f"Error parsing date '{date_str}': {e}")
            return None
