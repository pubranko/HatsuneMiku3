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
from models.controller_model import ControllerModel


def scrapy_deco(func):
    def deco(kwargs: dict):
        ### 初期処理 ###
        logger: Logger = kwargs['logger']
        ### 主処理 ###
        func(kwargs)
        ### 終了処理 ###
        # Scrapy実行後に、rootロガーに追加されているストリームハンドラを削除(これをやらないとログが二重化する)
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            if type(handler) == StreamHandler:
                root_logger.removeHandler(handler)
        root_logger = logging.getLogger()
        logger.info('=== 不要なのroot logger handlers 削除後の確認:' +
                    str(root_logger.handlers))
    return deco


@scrapy_deco
def direct_crawl_run(kwargs: dict):
    '''
    クロールするurlを直接指定。
    '''
    start_time: datetime = kwargs['start_time']
    urls: list = kwargs['urls']

    process = CrawlerProcess(settings=get_project_settings())
    configure_logging(install_root_handler=False)
    process.crawl(kwargs['class_instans'],
                  crawling_start_time=start_time,
                  direct_crawl_urls=urls,
                  crawl_point_non_update='Yes')
    process.start()


@scrapy_deco
def continued_run(kwargs: dict):
    '''
    前回の続きからクロールさせるようScrapyを実行する(多重起動)。
    crawling_stop_domain_listに登録がある場合は対象外。
    前回情報がないdomain＆spiderは対象外。
    '''
    logger: Logger = kwargs['logger']
    start_time: datetime = kwargs['start_time']
    controller: ControllerModel = kwargs['controller']
    spiders_info: list = kwargs['spiders_info']

    stop_domain: list = controller.crawling_stop_domain_list_get()

    process = CrawlerProcess(settings=get_project_settings())
    configure_logging(install_root_handler=False)

    for spider in spiders_info:
        spider: dict
        spider_domain: str = spider['domain']
        spider_domain_name: str = spider['domain_name']
        spider_name: str = spider['spider_name']

        if spider_domain in stop_domain:
            logger.info('=== Stop domainの指定によりクロール中止 : ドメイン(' +
                        spider_domain + ') : spider_name(' + spider_name + ')')
        else:
            next_point_record: dict = controller.crawl_point_get(
                spider_domain_name, spider_name,)
            if len(next_point_record):
                process.crawl(spider['class_instans'],
                              crawling_start_time=start_time,
                              continued='Yes')
    process.start()
