# -*- coding: utf-8 -*-

import logging
from influxdb import InfluxDBClient
from .utils import check_blank
from datetime import datetime, timezone
from .settings import INFLUX_DB_USER, INFLUX_DB_PWD, INFLUX_DB_HOST, INFLUX_DB_PORT
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

logger = logging.getLogger('CrawlerPipeline')


class CrawlerPipeline(object):
    name = "jinan_price"
    client = None

    def __init__(self):
        self.client = InfluxDBClient(INFLUX_DB_HOST, INFLUX_DB_PORT, INFLUX_DB_USER, INFLUX_DB_PWD,database=self.name)

    def process_item(self, item, spider):
        # 校验数据
        category = item['category']
        goods = item['goods']
        specification = item['specification']
        unit = item['unit']
        market = item['market']
        price = item['price']
        date = item['date']
        if check_blank(goods):
            return
        timestmp = datetime.strptime(date, '%Y/%m/%d %H:%M:%S').timestamp()
        item_datetime_str = datetime.utcfromtimestamp(timestmp).strftime('%Y-%m-%dT%H:%M:%SZ')
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
                "time": item_datetime_str,
                "fields": {
                    "price": float(price)
                }
            }
        ]
        self.client.write_points(json_body)
        return item
