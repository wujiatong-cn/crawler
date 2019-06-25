# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class CrawlerItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    category = scrapy.Field()
    goods = scrapy.Field()
    specification = scrapy.Field()
    unit = scrapy.Field()
    market = scrapy.Field()
    price = scrapy.Field()
    date = scrapy.Field(serializer=str)
