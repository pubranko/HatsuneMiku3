import os
import sys
import logging
from logging import Logger, StreamHandler
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


def regular_crawler_run(crawl_start_time):

    logger = logging.getLogger('prefect.' +
                      sys._getframe().f_code.co_name)
    process = CrawlerProcess(settings=get_project_settings())
    configure_logging(install_root_handler=False)
    process.crawl(EpochtimesJpSitemapSpider,
                  crawl_start_time=crawl_start_time,
                  debug='Yes',
                  lastmod_recent_time='30',)
    process.crawl(SankeiComSitemapSpider,
                  crawl_start_time=crawl_start_time,
                  debug='Yes',
                  lastmod_recent_time='30',
                  url_term_days='1',)
    process.start()

    # Scrapy実行後に、rootロガーに追加されているストリームハンドラを削除(これをやらないとログが二重化する)
    root_logger = logging.getLogger()
    handlers = root_logger.handlers
    for handler in root_logger.handlers:
        if type(handler) == StreamHandler:
            root_logger.removeHandler(handler)

    root_logger = logging.getLogger()
    handlers = root_logger.handlers
    logger.info('=== 不要なのroot logger handlers 削除後の確認:' + str(handlers))
