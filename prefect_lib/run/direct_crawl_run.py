import os
import sys
import logging
from logging import Logger, StreamHandler
from datetime import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from scrapy.utils.log import configure_logging
from typing import Union, Any, TYPE_CHECKING

path = os.getcwd()
sys.path.append(path)
from news_crawl.spiders.epochtimes_jp_sitemap import EpochtimesJpSitemapSpider
from news_crawl.spiders.sankei_com_sitemap import SankeiComSitemapSpider
from news_crawl.spiders.asahi_com_xml_feed import AsahiComXmlFeedSpider
from news_crawl.spiders.jp_reuters_com_sitemap import JpReutersComSitemapSpider
from news_crawl.spiders.kyodo_co_jp_sitemap import KyodoCoJpSitemapSpider
from news_crawl.spiders.yomiuri_co_jp_sitemap import YomiuriCoJpSitemapSpider

from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider
from news_crawl.spiders.extensions_class.extensions_crawl import ExtensionsCrawlSpider
from news_crawl.spiders.extensions_class.extensions_xml_feed import ExtensionsXmlFeedSpider


def crawling_deco(func):
    def exec(
        start_time: datetime,
        urls: list,
        class_instans: Union[ExtensionsSitemapSpider,
                             ExtensionsCrawlSpider, ExtensionsXmlFeedSpider]
    ):
        logger = logging.getLogger('prefect.run.crawling_deco')
        logger.info('=== crawling_deco / exec : ' +
                    str(start_time))

        # 主処理
        func(start_time, urls, class_instans)
        # 終了処理
        # Scrapy実行後に、rootロガーに追加されているストリームハンドラを削除(これをやらないとログが二重化する)
        root_logger = logging.getLogger()

        for handler in root_logger.handlers:
            if type(handler) == StreamHandler:
                root_logger.removeHandler(handler)

        root_logger = logging.getLogger()
        logger.info('=== 不要なのroot logger handlers 削除後の確認:' +
                    str(root_logger.handlers))
    return exec


@crawling_deco
def exec(
    start_time: datetime,
    urls: list,
    class_instans: Union[ExtensionsSitemapSpider,
                         ExtensionsCrawlSpider, ExtensionsXmlFeedSpider]
):
    ''''''
    process = CrawlerProcess(settings=get_project_settings())
    configure_logging(install_root_handler=False)
    process.crawl(class_instans,
                  crawling_start_time=start_time,
                  direct_crawl_urls=urls,
                  crawl_point_non_update='Yes')
    process.start()
