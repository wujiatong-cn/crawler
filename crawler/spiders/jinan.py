# -*- coding: utf-8 -*-
import scrapy

class DmozSpider(scrapy.Spider):
    name = "jinan_price"
    allowed_domains = ["119.163.120.130"]
    start_urls = [
        "http://119.163.120.130:88/allow/allow_1/Default.aspx"
    ]

    def parse(self, response):
        filename = response.url.split("/")[-2] + '.html'
        print(response)
        print("======================")