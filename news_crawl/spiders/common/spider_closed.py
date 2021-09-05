from datetime import datetime
from logging import Logger
from scrapy.statscollectors import MemoryStatsCollector
from models.mongo_model import MongoModel
from models.controller_model import ControllerModel
from models.crawler_logs_model import CrawlerLogsModel
from common_lib.resource_check import resource_check


def spider_closed(spider):
    '''spider共通の終了処理'''
    mongo: MongoModel = spider.mongo
    domain_name: str = spider._domain_name
    name: str = spider.name
    crawl_point: dict = spider._crawl_point
    logger: Logger = spider.logger
    stats: MemoryStatsCollector = spider.crawler.stats
    crawling_start_time: datetime = spider._crawling_start_time
    kwargs:dict = spider.kwargs_save

    if 'crawl_point_non_update' in kwargs:
        logger.info(
            '=== closed : 次回クロールポイント情報の更新Skip')
    else:
        controller = ControllerModel(mongo)
        controller.crawl_point_update(domain_name, name, crawl_point)
        logger.info(
            '=== closed : controllerに次回クロールポイント情報を保存 \n %s', crawl_point)

    resource_check()

    crawler_logs = CrawlerLogsModel(mongo)
    crawler_logs.insert_one({
        'crawling_start_time': crawling_start_time.isoformat(),
        'record_type': 'spider_stats',
        'domain_name': domain_name,
        'spider_name': name,
        'stats': stats.get_stats(),
    })

    mongo.close()
    logger.info('=== Spider closed: %s', name)
