# -*- coding: utf-8 -*-
import scrapy
import threading
from ..items import CrawlerItem
from influxdb import InfluxDBClient
from ..settings import INFLUX_DB_USER, INFLUX_DB_PWD, INFLUX_DB_HOST, INFLUX_DB_PORT
from ..utils import str_to_utc_datetime, get_current_utc_datetime, check_blank


class DmozSpider(scrapy.Spider):
    name = "jinan_price"
    allowed_domains = ["119.163.120.130"]
    start_urls = [
        "http://119.163.120.130:88/allow/allow_1/Default.aspx",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=2",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10002&m=1",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10002&m=2",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10115&m=1",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10115&m=2",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10017&m=1",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10017&m=2",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10016&m=1",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10016&m=2",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10015&m=1",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10015&m=2",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10125&m=1",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10125&m=2",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10145&m=1",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10145&m=2",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10135&m=1",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10135&m=2",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10155&m=1",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10155&m=2",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10175&m=1",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10175&m=2",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10165&m=1",
        "http://119.163.120.130:88/allow/allow_1/Default.aspx?d=10165&m=1",
    ]
    _client = None
    lock = threading.Lock()
    page_type_main_vegetable = ['主要蔬菜价格']
    page_type_main_agricultural = ['主要农副产品价格']

    def get_client(self):
        self.lock.acquire()
        if self._client is None:
            client = InfluxDBClient(INFLUX_DB_HOST, INFLUX_DB_PORT, INFLUX_DB_USER, INFLUX_DB_PWD)
            database_list = client.get_list_database()
            if {'name': self.name} not in database_list:
                client.create_database(self.name)
            client.switch_database(self.name)
            self._client = client
        self.lock.release()
        return self._client

    def start_requests(self):
        # yield scrapy.Request(self.start_urls[0], callback=self.parse_index_page)
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse_index_page)

    def parse_index_page(self, response):
        option_list = response.selector.xpath('//*[@id="ctl00_DropDownList1"]/option')[0:2]  # 切片控制爬数据的深度
        for option in option_list:
            date = option.xpath('@value').extract_first()
            category = response.selector.xpath('//*[@id="ctl00_Label1"]/text()').extract_first()
            self.logger.info("Found statistical date: %s", date)
            # 检查数据是不是处理过
            flag = False  # 不处理
            try:
                flag = self.judge_category_record(category, date)
            except Exception as e:
                self.logger.error(e)
                pass
            if not flag:
                self.logger.info("日期：%s ，分类: %s 的数据已经处理完成，或者最近1h发起过处理任务，本次不处理...", date, category)
                continue
            else:
                VIEWSTATE = response.selector.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first()
                VIEWSTATEGENERATOR = response.selector.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first()
                EVENTVALIDATION = response.selector.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first()
                request = scrapy.FormRequest(response.url, formdata={'__EVENTTARGET': 'ctl00$DropDownList1',
                                                                           '__VIEWSTATE': VIEWSTATE,
                                                                           '__VIEWSTATEGENERATOR': VIEWSTATEGENERATOR,
                                                                           '__EVENTVALIDATION': EVENTVALIDATION,
                                                                           'ctl00$DropDownList1': date},
                                             callback=self.parse_page_data)
                yield request

    def parse_page_data(self, response):
        date = response.selector.xpath(
            '//*[@id="ctl00_DropDownList1"]/option[contains(@selected,"selected")]/@value').extract_first()
        category = response.selector.xpath('//*[@id="ctl00_Label1"]/text()').extract_first()
        table = response.selector.xpath('//*[@id="ctl00_GridView1"]/tr')
        for index, tr in enumerate(table):
            td_length = len(tr.xpath('td'))

            if index == 0 or td_length < 4:
                continue
            goods = tr.xpath('td[1]/text()').extract_first()
            specification = tr.xpath('td[2]/text()').extract_first()
            unit = tr.xpath('td[3]/text()').extract_first()
            # 处理场景1界面，主要蔬菜价格
            if category in self.page_type_main_vegetable:
                for i in range(4, td_length + 1):
                    market = table[0].xpath('th[{0}]/text()'.format(i)).extract_first()
                    price = tr.xpath('td[{0}]/text()'.format(i)).extract_first()
                    # 第一条记录，保存到record表中
                    if index == 1 and i == 4:
                        self.record_gather_info(category, date, True)
                    # 最后一条记录
                    if index == len(table) - 1 and i == td_length:
                        self.record_gather_info(category, date, False)

                    if not check_blank(price):
                        item = CrawlerItem(goods=goods, specification=specification, unit=unit, market=market,
                                           price=price,
                                           date=date, category=category)
                        yield item
            # 处理场景2界面，主要农副产品价格
            elif category in self.page_type_main_agricultural:
                for item in [4, 7]:
                    market = table[0].xpath('th[{0}]/text()'.format(item)).extract_first()
                    price = tr.xpath('td[{0}]/text()'.format(item)).extract_first()
                    # 第一条记录，保存到record表中
                    if index == 1 and item == 4:
                        self.record_gather_info(category, date, True)
                    # 最后一条记录
                    if index == len(table) - 1 and item == 7:
                        self.record_gather_info(category, date, False)

                    if not check_blank(price):
                        item = CrawlerItem(goods=goods, specification=specification, unit=unit, market=market,
                                           price=price,
                                           date=date, category=category)
                        yield item
            # 处理普通页面
            else:
                market = category
                price = tr.xpath('td[4]/text()').extract_first()
                # 第一条记录，保存到record表中
                if index == 1:
                    self.record_gather_info(category, date, True)
                # 最后一条记录
                if index == len(table) - 1:
                    self.record_gather_info(category, date, False)

                if not check_blank(price):
                    item = CrawlerItem(goods=goods, specification=specification, unit=unit, market=market,
                                       price=price,
                                       date=date, category=category)
                    yield item

    # 将处理页面数据的记录，存到record表中
    def record_gather_info(self, category, date, start=True):
        self.logger.info("分类：%s ,日期：%s ,%s ...", category, date, "开始处理" if start else "处理完成")
        json_body = [
            {
                "measurement": "record",
                "tags": {
                    "category": category,
                    "page_time": str_to_utc_datetime(date),  # 页面上的数据时间
                },
                "time": get_current_utc_datetime(),  # 记录时间
                "fields": {
                    "operate_code": 1 if start else 2
                }
            }
        ]
        self.get_client().write_points(json_body)

    # 查询记录表，判断是否需要处理该页面。
    # 如果需要处理，返回True，否则返回False
    def judge_category_record(self, category, date):
        flag = False
        # 查询当前类别是否已经处理完成
        judge_finish_sql = 'SELECT * FROM record WHERE category = \'{0}\'  AND operate_code = 2 AND page_time = \'{1}\';'.format(
            category, str_to_utc_datetime(date))
        finish_result = self.get_client().query(judge_finish_sql)
        # 处理完成，不再处理
        if not finish_result:
            # 查询有没有最近1小时开始开始的解析任务（9h-8h 时区问题）
            judge_start_sql = 'SELECT * FROM record WHERE category = \'{0}\'  AND operate_code = 1 AND page_time = \'{1}\' AND time > now() - 9h;'.format(
                category, str_to_utc_datetime(date))
            start_result = self.get_client().query(judge_start_sql)
            # 任务开始了超过一个小时了，大概是废了。重新处理
            # 有解析任务，可能正在进行，不再处理
            if not start_result:
                flag = True
        return flag
