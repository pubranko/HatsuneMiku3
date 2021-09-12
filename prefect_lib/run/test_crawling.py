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
from news_crawl.spiders.asahi_com_sitemap import AsahiComSitemapSpider
from news_crawl.spiders.asahi_com_xml_feed import AsahiComXmlFeedSpider
from news_crawl.spiders.jp_reuters_com_sitemap import JpReutersComSitemapSpider
from news_crawl.spiders.kyodo_co_jp_sitemap import KyodoCoJpSitemapSpider
from news_crawl.spiders.yomiuri_co_jp_sitemap import YomiuriCoJpSitemapSpider


def crawling_deco(func):
    def exec(start_time: datetime):
        # 主処理
        func(start_time)
        # 終了処理
        # Scrapy実行後に、rootロガーに追加されているストリームハンドラを削除(これをやらないとログが二重化する)
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if type(handler) == StreamHandler:
                root_logger.removeHandler(handler)

        root_logger = logging.getLogger()
        logger = logging.getLogger('prefect.run.crawling_deco')
        logger.info('=== 不要なのroot logger handlers 削除後の確認:' +
                    str(root_logger.handlers))
    return exec


@crawling_deco
def test5(start_time: datetime):
    '''正常：データ（直近60分）'''
    process = CrawlerProcess(settings=get_project_settings())
    configure_logging(install_root_handler=False)
    process.crawl(EpochtimesJpSitemapSpider,
                  crawling_start_time=start_time,
                  debug='Yes',
                  lastmod_period_minutes='60,0',)
    process.crawl(SankeiComSitemapSpider,
                  crawling_start_time=start_time,
                  debug='Yes',
                  lastmod_period_minutes='60,0',)
    process.crawl(AsahiComSitemapSpider,
                  crawling_start_time=start_time,
                  debug='Yes',
                  lastmod_period_minutes='60,0',)
    process.start()
