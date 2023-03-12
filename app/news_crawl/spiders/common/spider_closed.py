from __future__ import annotations  # ExtensionsSitemapSpiderの循環参照を回避するため
from typing import Union, TYPE_CHECKING
from datetime import datetime
from scrapy.statscollectors import MemoryStatsCollector
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from BrownieAtelierMongo.collection_models.crawler_logs_model import CrawlerLogsModel
from shared.resource_check import resource_check

if TYPE_CHECKING:  # 型チェック時のみインポート
    from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider
    from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
    #from news_crawl.spiders.extensions_class.extensions_xml_feed import ExtensionsXmlFeedSpider


def spider_closed(
    spider: Union[ExtensionsSitemapSpider, ExtensionsCrawlSpider],
):
    '''spider共通の終了処理'''
    stats: MemoryStatsCollector = spider.crawler.stats

    if spider.news_crawl_input.crawl_point_non_update:
        spider.logger.info(
            '=== closed : 次回クロールポイント情報の更新Skip')
    else:
        controller = ControllerModel(spider.mongo)
        controller.crawl_point_update(spider._domain_name, spider.name, spider._crawl_point)
        spider.logger.info(
            f'=== closed : controllerに次回クロールポイント情報を保存 \n {spider._crawl_point}')

    resource_check()

    # クロールの統計結果とクロールを行ったサイトの一覧情報を「spider_report」としてログに保存する。
    crawler_logs = CrawlerLogsModel(spider.mongo)

    crawler_logs.spider_report_insert(
        spider.news_crawl_input.crawling_start_time,
        spider.allowed_domains[0],
        spider.name,
        stats,
        spider.crawl_urls_list,)


    spider.mongo.close()
    spider.logger.info(f'=== Spider closed: {spider.name}')
