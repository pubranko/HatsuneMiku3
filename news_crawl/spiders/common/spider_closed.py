from datetime import datetime
from typing import Union
from scrapy.statscollectors import MemoryStatsCollector
from scrapy.spiders import Spider
from news_crawl.models.mongo_model import MongoModel
from news_crawl.models.crawler_controller_model import CrawlerControllerModel
from news_crawl.models.crawler_logs_model import CrawlerLogsModel
from logging import LoggerAdapter


def spider_closed(spider):
    '''spider共通の終了処理'''
    mongo: MongoModel = spider.mongo
    domain_name: str = spider._domain_name
    name: str = spider.name
    next_crawl_point: dict = spider._next_crawl_point
    logger: LoggerAdapter = spider.logger
    stats: MemoryStatsCollector = spider.crawler.stats
    crawl_start_time: datetime = spider._crawl_start_time

    crawler_controller = CrawlerControllerModel(mongo)
    crawler_controller.next_crawl_point_update(
        domain_name, name, next_crawl_point)

    logger.info(
        '=== closed : crawler_controllerに次回クロールポイント情報を保存 \n %s', next_crawl_point)

    crawler_logs = CrawlerLogsModel(mongo)
    crawler_logs.insert_one({
        'crawl_start_time': crawl_start_time.isoformat(),
        'record_type': 'spider_stats',
        'domain_name': domain_name,
        'spider_name': name,
        'stats': stats.get_stats(),
    })

    mongo.close()
    logger.info('=== Spider closed: %s', name)
