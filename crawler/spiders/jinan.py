# -*- coding: utf-8 -*-
import scrapy
from ..items import CrawlerItem
from influxdb import InfluxDBClient
from ..settings import INFLUX_DB_USER, INFLUX_DB_PWD, INFLUX_DB_HOST, INFLUX_DB_PORT
from ..utils import str_to_utc_datetime


class DmozSpider(scrapy.Spider):
    name = "jinan_price"
    allowed_domains = ["119.163.120.130"]
    start_urls = [
        "http://119.163.120.130:88/allow/allow_1/Default.aspx"
    ]

    def start_requests(self):
        yield scrapy.Request(self.start_urls[0], callback=self.parse_index_page)

    def parse_index_page(self, response):
        option_list = response.selector.xpath('//*[@id="ctl00_DropDownList1"]/option')

        for option in option_list:
            date = option.xpath('@value').extract_first()
            category = response.selector.xpath('//*[@id="ctl00_Label1"]/b/font/text()').extract_first()
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
                sql = 'SELECT * FROM record WHERE category = \'{0}\' AND operate_code = 1 AND time = \'{1}\';'.format(category, str_to_utc_datetime(date))
                result = client.query(sql)
                if result:
                    flag = True
            except Exception as e:
                pass
            if flag:
                self.logger.info("日期：%s ，分类: %s 的数据已经处理过，跳过...", date, category)
                break
            else:
                VIEWSTATE = response.selector.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first()
                VIEWSTATEGENERATOR = response.selector.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first()
                EVENTVALIDATION = response.selector.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first()
                request = scrapy.FormRequest(self.start_urls[0], formdata={'__EVENTTARGET': 'ctl00$DropDownList1',
                                                                           '__VIEWSTATE': VIEWSTATE,
                                                                           '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
                                                                           '__EVENTVALIDATION': EVENTVALIDATION,
                                                                           'ctl00$DropDownList1': date},
                                             callback=self.parse_page_data)
                yield request

    def parse_page_data(self, response):
        date = response.selector.xpath('//*[@id="ctl00_DropDownList1"]/option[contains(@selected,"selected")]/@value').extract_first()
        category = response.selector.xpath('//*[@id="ctl00_Label1"]/b/font/text()').extract_first()
        table = response.selector.xpath('//*[@id="ctl00_GridView1"]/tr')
        first_data_point = True
        last_data_point = False
        for index, tr in enumerate(table):
            if index == 0 or len(tr.xpath('td')) < 4:
                continue
            for i in range(4, len(tr.xpath('td'))):
                goods = tr.xpath('td[1]/text()').extract_first()
                specification = tr.xpath('td[2]/text()').extract_first()
                unit = tr.xpath('td[3]/text()').extract_first()
                market = table[0].xpath('th[{0}]/text()'.format(i)).extract_first()
                price = tr.xpath('td[{0}]/text()'.format(i)).extract_first()
                if price == " " or price == "" or price is None:
                    continue
                # self.logger.info("抓取商品：%s %s %s, 市场：%s, 价格：%s, 日期：%s.", goods, specification, unit, market, price, date)

                # 记录到record表中
                if first_data_point:
                    self.record_gather_info(category, date)
                    first_data_point = False
                item = CrawlerItem(goods=goods, specification=specification, unit=unit, market=market, price=price, date=date, category=category)
                yield item

    # 将处理页面数据的记录，存到record表中
    # 此功能需要继续完善，页面最后一条数据处理完后，添加一条operate_code = 2 的记录表示处理完成。
    # 检查处理信息时，除了要检查是否又开始记录（operate_code = 1），还要考虑是否已经处理完成（operate_code = 2 ）。
    # 有开始记录，没有完成记录，且开始记录的时间已经超过某个时间（30min?）可能是失败了，需要重新处理这个页面的数据
    def record_gather_info(self, category, date):
        client = InfluxDBClient(INFLUX_DB_HOST, INFLUX_DB_PORT, INFLUX_DB_USER, INFLUX_DB_PWD, self.name)
        json_body = [
            {
                "measurement": "record",
                "tags": {
                    "category": category,
                    "page_time": "", # 页面上的数据时间
                },
                "time": str_to_utc_datetime(date), # 这一列应该存记录时间
                "fields": {
                    "operate_code": 1
                }
            }
        ]
        client.write_points(json_body)