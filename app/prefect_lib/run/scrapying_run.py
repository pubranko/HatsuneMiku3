import os
import sys
import logging
import pickle
from typing import Any
from logging import Logger
from datetime import datetime
from importlib import import_module
from pymongo import ASCENDING
from pymongo.cursor import Cursor
from bs4 import BeautifulSoup as bs4
from bs4.element import Tag
from bs4.element import ResultSet
path = os.getcwd()
sys.path.append(path)
from BrownieAtelierMongo.models.mongo_model import MongoModel
from BrownieAtelierMongo.models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.models.scraped_from_response_model import ScrapedFromResponseModel
from BrownieAtelierMongo.models.scraper_info_by_domain_model import ScraperInfoByDomainModel
from BrownieAtelierMongo.models.controller_model import ControllerModel
from shared.timezone_recovery import timezone_recovery
from prefect_lib.common_module.scraped_record_error_check import scraped_record_error_check

logger: Logger = logging.getLogger('prefect.run.scrapying_deco')


def exec(kwargs: dict):
    '''あとで'''
    global logger
    start_time: datetime = kwargs['start_time']
    mongo: MongoModel = kwargs['mongo']
    crawler_response: CrawlerResponseModel = CrawlerResponseModel(mongo)
    scraped_from_response: ScrapedFromResponseModel = ScrapedFromResponseModel(
        mongo)
    scraper_by_domain: ScraperInfoByDomainModel = ScraperInfoByDomainModel(mongo)
    controller: ControllerModel = ControllerModel(mongo)
    domain: str = kwargs['domain']
    crawling_start_time_from: datetime = kwargs['crawling_start_time_from']
    crawling_start_time_to: datetime = kwargs['crawling_start_time_to']
    urls: list = kwargs['urls']

    stop_domain: list = controller.scrapying_stop_domain_list_get()

    conditions: list = []
    if domain:
        conditions.append({'domain': domain})
    if crawling_start_time_from:
        conditions.append(
            {'crawling_start_time': {'$gte': crawling_start_time_from}})
    if crawling_start_time_to:
        conditions.append(
            {'crawling_start_time': {'$lte': crawling_start_time_to}})
    if urls:
        conditions.append({'url': {'$in': urls}})
    if len(stop_domain) > 0:
        conditions.append({'domain': {'$nin': stop_domain}})

    if conditions:
        filter: Any = {'$and': conditions}
    else:
        filter = None
    logger.info(f'=== crawler_responseへのfilter: {str(filter)}')

    # スクレイピング対象件数を確認
    record_count = crawler_response.count(filter=filter)
    logger.info(f'=== crawler_response スクレイピング対象件数 : {str(record_count)}')

    # 件数制限でスクレイピング処理を実施
    limit: int = 100
    skip_list = list(range(0, record_count, limit))
    scraped: dict = {}
    scraper_mod: dict = {}
    old_domain:str = ''

    for skip in skip_list:
        records: Cursor = crawler_response.find(
            projection=None,
            filter=filter,
            sort=[('domain', ASCENDING),('response_time', ASCENDING)],
        ).skip(skip).limit(limit)

        for record in records:
            # 各サイト共通の項目を設定
            scraped = {}
            scraped['domain'] = record['domain']
            scraped['url'] = record['url']
            scraped['response_time'] = timezone_recovery(
                record['response_time'])
            scraped['crawling_start_time'] = timezone_recovery(
                record['crawling_start_time'])
            scraped['scrapying_start_time'] = start_time
            scraped['source_of_information'] = record['source_of_information']

            # response_bodyをbs4で解析
            response_body: str = pickle.loads(record['response_body'])
            soup: bs4 = bs4(response_body, 'lxml')
            scraped['pattern'] = {}

            # ドメイン別スクレイパー情報をDBより取得
            if not old_domain == record['domain']:
                scraper_by_domain.record_read(filter={'domain': record['domain']})
                logger.info(f'=== scrapying_run run  ドメイン別スクレイパー情報取得 (domain: {record["domain"]})')

            for scraper, pattern_list in scraper_by_domain.scrape_item_get():
                if not scraper in scraper_mod:
                    scraper_mod[scraper] = import_module(
                        'prefect_lib.scraper.' + scraper)
                scraped_result, scraped_pattern = getattr(scraper_mod[scraper], 'scraper')(
                    soup=soup, scraper=scraper, scrape_parm=pattern_list,)
                scraped.update(scraped_result)
                scraped['pattern'].update(scraped_pattern)
            #print('date: ',scraped['publish_date'])
            #print('article: ',scraped['article'])

            # データチェック
            error_flg: bool = scraped_record_error_check(scraped)
            if not error_flg:
                scraped_from_response.insert_one(scraped)
                logger.info(f'=== scrapying_run run  処理対象url : {record["url"]}')

            # 最後に今回処理を行ったdomainを保存
            old_domain = record['domain']
