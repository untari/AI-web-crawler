# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from datetime import datetime


class CrawlerItem(scrapy.Item):
    name = scrapy.Field()
    pass

class MarketItem(scrapy.Item):

    unique_id = scrapy.Field() 
    date = scrapy.Field()
    label = scrapy.Field()
    header = scrapy.Field()
    sub_header = scrapy.Field()
    img = scrapy.Field()
    img_caption = scrapy.Field()
    content = scrapy.Field()
