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
    def exec(starting_time: datetime, mongo: MongoModel):
        ### 主処理
        func(starting_time, mongo)
        ### 終了処理
        # Scrapy実行後に、rootロガーに追加されているストリームハンドラを削除(これをやらないとログが二重化する)
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if type(handler) == StreamHandler:
                root_logger.removeHandler(handler)

        root_logger = logging.getLogger()
        logger = logging.getLogger('prefect.run.scrapy_deco.' +
                                   sys._getframe().f_code.co_name)
        logger.info('=== 不要なのroot logger handlers 削除後の確認:' +
                    str(root_logger.handlers))
    return exec


@crawling_deco
def test1(starting_time: datetime, mongo: MongoModel):
    '''正常：データほぼ無し（直近1分）'''
    process = CrawlerProcess(settings=get_project_settings())
    configure_logging(install_root_handler=False)
    process.crawl(EpochtimesJpSitemapSpider,
                  crawl_start_time=starting_time,
                  debug='Yes',
                  lastmod_recent_time='1',)
    process.crawl(SankeiComSitemapSpider,
                  crawl_start_time=starting_time,
                  debug='Yes',
                  lastmod_recent_time='1',
                  url_term_days='1',)
    process.start()


@crawling_deco
def test2(starting_time: datetime, mongo: MongoModel):
    '''エラーケース'''
    process = CrawlerProcess(settings=get_project_settings())
    configure_logging(install_root_handler=False)
    process.crawl(EpochtimesJpSitemapSpider,
                  crawl_start_time=starting_time,
                  debug='Yes!',
                  lastmod_recent_time='1',)
    process.crawl(SankeiComSitemapSpider,
                  crawl_start_time=starting_time,
                  debug='Yes',
                  lastmod_recent_time='1a',
                  url_term_days='1',)
    process.start()

@crawling_deco
def test3(starting_time: datetime, mongo: MongoModel):
    '''正常：データほぼ無し（直近1分）'''
    process = CrawlerProcess(settings=get_project_settings())
    configure_logging(install_root_handler=False)
    process.crawl(EpochtimesJpSitemapSpider,
                  crawl_start_time=starting_time,
                  debug='Yes',
                  lastmod_recent_time='60',)
    process.crawl(SankeiComSitemapSpider,
                  crawl_start_time=starting_time,
                  debug='Yes',
                  lastmod_recent_time='60',
                  url_term_days='1',)
    process.crawl(AsahiComXmlFeedSpider,
                  crawl_start_time=starting_time,
                  debug='Yes',
                  lastmod_recent_time='60',)
    process.start()

@crawling_deco
def test4(starting_time: datetime, mongo: MongoModel):
    '''正常：データほぼ無し（直近1分）'''
    process = CrawlerProcess(settings=get_project_settings())
    configure_logging(install_root_handler=False)
    process.crawl(EpochtimesJpSitemapSpider,
                  crawl_start_time=starting_time,
                  debug='Yes',
                  lastmod_recent_time='180',)
    process.start()

