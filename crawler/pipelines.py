# -*- coding: utf-8 -*-

import logging
from influxdb import InfluxDBClient
from .utils import check_blank, str_to_utc_datetime
from .settings import INFLUX_DB_USER, INFLUX_DB_PWD, INFLUX_DB_HOST, INFLUX_DB_PORT
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

logger = logging.getLogger('CrawlerPipeline')


class CrawlerPipeline(object):

    def __init__(self):
        self.client = InfluxDBClient(INFLUX_DB_HOST, INFLUX_DB_PORT, INFLUX_DB_USER, INFLUX_DB_PWD)

    def process_item(self, item, spider):
        # 校验数据
        category = item['category']
        goods = item['goods']
        specification = item['specification']
        unit = item['unit']
        market = item['market']
        price = item['price']
        date = str_to_utc_datetime(item['date'])
        if check_blank(goods):
            return

        # 发送到数据库
        # 检查数据库是否有记录，没有则需要在record表中记录信息
        json_body = [
            {
                "measurement": "price",
                "tags": {
                    "category": category,
                    "goods": goods,
                    "specification": specification,
                    "unit": unit,
                    "market": market
                },
                "time": date,
                "fields": {
                    "price": float(price)
                }
            }
        ]
        # todo: InfluxDB-Python似乎没有提供后台线程写入数据，后面考虑优化批量写入数据点
        self.client.write_points(json_body, database=spider.name)
        return item
