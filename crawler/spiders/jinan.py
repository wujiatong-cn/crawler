# -*- coding: utf-8 -*-
import scrapy
from influxdb import InfluxDBClient
from ..settings import INFLUX_DB_USER, INFLUX_DB_PWD, INFLUX_DB_HOST, INFLUX_DB_PORT


class DmozSpider(scrapy.Spider):
    name = "jinan_price"
    allowed_domains = ["119.163.120.130"]
    start_urls = [
        "http://119.163.120.130:88/allow/allow_1/Default.aspx"
    ]

    def start_requests(self):
        return [scrapy.Request(self.start_urls[0], callback=self.parse_index_page)]

    def parse_index_page(self, response):
        option_list = response.selector.xpath('//*[@id="ctl00_DropDownList1"]/option')

        for option in option_list:
            date = option.xpath('@value').extract_first()
            self.logger.info("Found statistical date: %s", date)
            # 查询获取到的日期是不是已经保存过，暂时保存过的日期列表存到influxDB record表中
            # record 表的结构
            # category | time | action
            client = InfluxDBClient(INFLUX_DB_HOST, INFLUX_DB_PORT, INFLUX_DB_USER, INFLUX_DB_PWD)
            database_list = client.get_list_database()
            if {'name': self.name} not in database_list:
                client.create_database(self.name)
            client.switch_database(self.name)
            flag = False  # 是否已经存过
            try:
                result = client.query('SELECT * FROM record WHERE time = {0};'.format(date))
                self.logger.info(result)
                if result:
                    flag = True
            except Exception as e:
                pass
            if flag:
                break
            else:
                return self.request_price_data(response, date)

    def request_price_data(self, response, date):
        VIEWSTATE = response.selector.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first()
        VIEWSTATEGENERATOR = response.selector.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first()
        EVENTVALIDATION = response.selector.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first()
        request = scrapy.FormRequest(self.start_urls[0], formdata={'__EVENTTARGET': 'ctl00$DropDownList1',
                                                                   '__VIEWSTATE': VIEWSTATE,
                                                                   '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
                                                                   '__EVENTVALIDATION': EVENTVALIDATION,
                                                                   'ctl00$DropDownList1': date},
                                     callback=self.parse_page_data)
        return [request]

    def parse_page_data(self, response):
        self.logger.info('=================================================')
        price = response.selector.xpath('//*[@id="ctl00_GridView1"]')
        self.logger.info(price)
