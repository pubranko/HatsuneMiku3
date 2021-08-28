import os
import sys
import logging
from logging import Logger, StreamHandler
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging

path = os.getcwd()
sys.path.append(path)
from news_crawl.spiders.epochtimes_jp_sitemap import EpochtimesJpSitemapSpider
from news_crawl.spiders.sankei_com_sitemap import SankeiComSitemapSpider
from news_crawl.spiders.asahi_com_xml_feed import AsahiComXmlFeedSpider
from news_crawl.spiders.jp_reuters_com_sitemap import JpReutersComSitemapSpider
from news_crawl.spiders.kyodo_co_jp_sitemap import KyodoCoJpSitemapSpider
from news_crawl.spiders.yomiuri_co_jp_sitemap import YomiuriCoJpSitemapSpider
from models.mongo_model import MongoModel


def crawling_deco(func):
    def deco(starting_time: datetime):
        ### 主処理
        func(starting_time)
        ### 終了処理
        # Scrapy実行後に、rootロガーに追加されているストリームハンドラを削除(これをやらないとログが二重化する)
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if type(handler) == StreamHandler:
                root_logger.removeHandler(handler)

        root_logger = logging.getLogger()
        logger = logging.getLogger('prefect.run.crawling_deco')
        logger.info('=== 不要なのroot logger handlers 削除後の確認:' +
                    str(root_logger.handlers))
    return deco


@crawling_deco
def exec(starting_time: datetime):
    '''正常：データ（直近60分）'''
    process = CrawlerProcess(settings=get_project_settings())
    configure_logging(install_root_handler=False)
    process.crawl(EpochtimesJpSitemapSpider,
                  crawl_start_time=starting_time,
                  continued='Yes')
    process.crawl(SankeiComSitemapSpider,
                  crawl_start_time=starting_time,
                  continued='Yes')
    process.crawl(AsahiComXmlFeedSpider,
                  crawl_start_time=starting_time,
                  continued='Yes')
    process.start()