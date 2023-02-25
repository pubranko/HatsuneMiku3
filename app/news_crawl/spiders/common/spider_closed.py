from __future__ import annotations  # ExtensionsSitemapSpiderの循環参照を回避するため
from typing import Union, Any, TYPE_CHECKING
from datetime import datetime
from logging import LoggerAdapter
from scrapy.statscollectors import MemoryStatsCollector
from BrownieAtelierMongo.models.mongo_model import MongoModel
from BrownieAtelierMongo.models.controller_model import ControllerModel
from BrownieAtelierMongo.models.crawler_logs_model import CrawlerLogsModel
from shared.resource_check import resource_check

if TYPE_CHECKING:  # 型チェック時のみインポート
    from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider
    from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
    #from news_crawl.spiders.extensions_class.extensions_xml_feed import ExtensionsXmlFeedSpider


def spider_closed(
    spider: Union[ExtensionsSitemapSpider, ExtensionsCrawlSpider],
):
    '''spider共通の終了処理'''
    mongo: MongoModel = spider.mongo
    domain_name: str = spider._domain_name
    name: str = spider.name
    crawl_point: dict = spider._crawl_point
    logger: LoggerAdapter = spider.logger
    stats: MemoryStatsCollector = spider.crawler.stats
    crawling_start_time: datetime = spider._crawling_start_time
    kwargs: dict = spider.kwargs_save

    if 'crawl_point_non_update' in kwargs and kwargs['crawl_point_non_update'] == 'Yes':
        logger.info(
            '=== closed : 次回クロールポイント情報の更新Skip')
    else:
        controller = ControllerModel(mongo)
        controller.crawl_point_update(domain_name, name, crawl_point)
        logger.info(
            f'=== closed : controllerに次回クロールポイント情報を保存 \n {crawl_point}')

    resource_check()

    stats_edit: dict = {}
    for item in stats.get_stats().items():
        key: str = str(item[0]).replace('.', '_')
        stats_edit[key] = item[1]

    # ログに保管するときはsource_urlごとの配列内にlocとlastmodを格納するよう構造変換
    temp: dict = {}
    source_urls:list = []
    for record in spider.crawl_urls_list:
        if record['source_url'] in temp.keys():
            temp[record['source_url']].append({
                'loc': record['loc'],
                'lastmod': record['lastmod']})
        else:
            temp[record['source_url']] = [{
                'loc': record['loc'],
                'lastmod': record['lastmod'], }]
    for key,value in temp.items():
        source_urls.append({
            'source_url':key,
            'items':value,
        })

    crawler_logs = CrawlerLogsModel(mongo)
    crawler_logs.insert_one({
        'start_time': crawling_start_time,
        'record_type': 'spider_reports',
        'domain': spider.allowed_domains[0],
        'spider_name': name,
        'stats': stats_edit,
        'crawl_urls_list': source_urls,
    })

    mongo.close()
    logger.info(f'=== Spider closed: {name}')
