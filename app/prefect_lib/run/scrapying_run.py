import os
import sys
import logging
import pickle
from typing import Any, Optional
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
from prefect_lib.common_module.scraped_record_error_check import scraped_record_error_check
from shared.timezone_recovery import timezone_recovery
from BrownieAtelierMongo.data_models.scraper_info_by_domain_data import ScraperInfoByDomainData
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from BrownieAtelierMongo.collection_models.scraper_info_by_domain_model import ScraperInfoByDomainModel
from BrownieAtelierMongo.collection_models.scraped_from_response_model import ScrapedFromResponseModel
from BrownieAtelierMongo.collection_models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.collection_models.mongo_model import MongoModel

logger: Logger = logging.getLogger('prefect.run.scrapying_deco')

# start_time,mongo,domain,target_start_time_from,target_start_time_to,
# def exec(kwargs: dict):
def exec(start_time: datetime,
         mongo: MongoModel,
         domain: Optional[str],
         urls: Optional[list[str]],
         target_start_time_from: Optional[datetime],
         target_start_time_to: Optional[datetime]):
    '''あとで'''
    global logger
    # start_time: datetime = kwargs['start_time']
    # mongo: MongoModel = kwargs['mongo']

    crawler_response: CrawlerResponseModel = CrawlerResponseModel(mongo)
    scraped_from_response: ScrapedFromResponseModel = ScrapedFromResponseModel(mongo)
    scraper_info_by_domain: ScraperInfoByDomainModel = ScraperInfoByDomainModel(mongo)
    controller: ControllerModel = ControllerModel(mongo)

    # domain: str = kwargs['domain']
    # crawling_start_time_from: datetime = kwargs['crawling_start_time_from']
    # crawling_start_time_to: datetime = kwargs['crawling_start_time_to']
    # crawling_start_time_from: datetime = target_start_time_from
    # crawling_start_time_to: datetime = target_start_time_to
    # urls: list = kwargs['urls']

    stop_domain: list = controller.scrapying_stop_domain_list_get()

    conditions: list = []
    if domain:
        conditions.append({crawler_response.DOMAIN: domain})
    if target_start_time_from:
        conditions.append(
            {crawler_response.CRAWLING_START_TIME: {'$gte': target_start_time_from}})
    if target_start_time_to:
        conditions.append(
            {crawler_response.CRAWLING_START_TIME: {'$lte': target_start_time_to}})
    if urls:
        conditions.append({crawler_response.URL: {'$in': urls}})
    if len(stop_domain) > 0:
        conditions.append({crawler_response.DOMAIN: {'$nin': stop_domain}})

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
    old_domain: str = ''

    for skip in skip_list:
        records: Cursor = crawler_response.find(
            projection=None,
            filter=filter,
            sort=[(crawler_response.DOMAIN, ASCENDING), (crawler_response.RESPONSE_TIME, ASCENDING)],
            # sort=[('domain', ASCENDING), ('response_time', ASCENDING)],
        ).skip(skip).limit(limit)

        for record in records:
            # 各サイト共通の項目を設定
            scraped:dict = {}
            scraped[scraper_info_by_domain.DOMAIN] = record[crawler_response.DOMAIN]
            scraped[scraped_from_response.URL] = record[crawler_response.URL]
            scraped[scraped_from_response.RESPONSE_TIME] = timezone_recovery(
                record[crawler_response.RESPONSE_TIME])
            scraped[scraped_from_response.CRAWLING_START_TIME] = timezone_recovery(
                record[crawler_response.CRAWLING_START_TIME])
            scraped[scraped_from_response.SCRAPYING_START_TIME] = start_time
            scraped[scraped_from_response.SOURCE_OF_INFORMATION] = record[crawler_response.SOURCE_OF_INFORMATION]

            # response_bodyをbs4で解析
            response_body: str = pickle.loads(record[crawler_response.RESPONSE_BODY])
            soup: bs4 = bs4(response_body, 'lxml')
            scraped[scraped_from_response.PATTERN] = {}

            # ドメイン別スクレイパー情報をDBより取得
            scraper_info_by_domain_data_list: list[ScraperInfoByDomainData] = []
            if not old_domain == record[crawler_response.DOMAIN]:
                scraper_info_by_domain_data_list = scraper_info_by_domain.find_and_data_models_get(
                    filter={scraper_info_by_domain.DOMAIN: record[crawler_response.DOMAIN]})
                # scraper_by_domain.record_read(filter={'domain': record['domain']})
                logger.info(
                    f'=== scrapying_run run  ドメイン別スクレイパー情報取得 (domain: {record[crawler_response.DOMAIN]})')

            scraper_info_by_domain_data = scraper_info_by_domain_data_list[0]   # ドメイン単位で取得しているため常に１件
            # for scraper, pattern_list in scraper_by_domain.scrape_item_get():
            for scraper, pattern_list in scraper_info_by_domain_data.scrape_item_get():
                if not scraper in scraper_mod:
                    scraper_mod[scraper] = import_module(
                        'prefect_lib.scraper.' + scraper)
                scraped_result, scraped_pattern = getattr(scraper_mod[scraper], 'scraper')(
                    soup=soup, scraper=scraper, scrape_parm=pattern_list,)
                scraped.update(scraped_result)
                scraped[scraped_from_response.PATTERN].update(scraped_pattern)
            #print('date: ',scraped['publish_date'])
            #print('article: ',scraped['article'])

            # データチェック
            error_flg: bool = scraped_record_error_check(scraped)
            if not error_flg:
                scraped_from_response.insert_one(scraped)
                logger.info(
                    f'=== scrapying_run run  処理対象url : {record[crawler_response.URL]}')

            # 最後に今回処理を行ったdomainを保存
            old_domain = record[crawler_response.DOMAIN]
