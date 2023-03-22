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
from BrownieAtelierMongo.collection_models.controller_model import ControllerModel
from BrownieAtelierMongo.collection_models.scraper_info_by_domain_model import ScraperInfoByDomainModel
from BrownieAtelierMongo.data_models.scraper_info_by_domain_data import ScraperInfoByDomainData, ScraperInfoByDomainConst
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

    crawler_response: CrawlerResponseModel = CrawlerResponseModel(mongo)
    scraped_from_response: ScrapedFromResponseModel = ScrapedFromResponseModel(mongo)
    scraper_info_by_domain: ScraperInfoByDomainModel = ScraperInfoByDomainModel(mongo)
    controller: ControllerModel = ControllerModel(mongo)

    stop_domain: list = controller.scrapying_stop_domain_list_get()

    conditions: list = []
    if domain:
        conditions.append({CrawlerResponseModel.DOMAIN: domain})
    if target_start_time_from:
        conditions.append(
            {CrawlerResponseModel.CRAWLING_START_TIME: {'$gte': target_start_time_from}})
    if target_start_time_to:
        conditions.append(
            {CrawlerResponseModel.CRAWLING_START_TIME: {'$lte': target_start_time_to}})
    if urls:
        conditions.append({CrawlerResponseModel.URL: {'$in': urls}})
    if len(stop_domain) > 0:
        conditions.append({CrawlerResponseModel.DOMAIN: {'$nin': stop_domain}})

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
    scraper_info_by_domain_data_list: list[ScraperInfoByDomainData] = []

    for skip in skip_list:
        records: Cursor = crawler_response.find(
            projection=None,
            filter=filter,
            sort=[(CrawlerResponseModel.DOMAIN, ASCENDING), (CrawlerResponseModel.RESPONSE_TIME, ASCENDING)],
            # sort=[('domain', ASCENDING), ('response_time', ASCENDING)],
        ).skip(skip).limit(limit)

        for record in records:
            # 各サイト共通の項目を設定
            scraped:dict = {}
            scraped[ScrapedFromResponseModel.DOMAIN] = record[CrawlerResponseModel.DOMAIN]
            scraped[ScrapedFromResponseModel.URL] = record[CrawlerResponseModel.URL]
            scraped[ScrapedFromResponseModel.RESPONSE_TIME] = timezone_recovery(
                record[CrawlerResponseModel.RESPONSE_TIME])
            scraped[ScrapedFromResponseModel.CRAWLING_START_TIME] = timezone_recovery(
                record[CrawlerResponseModel.CRAWLING_START_TIME])
            scraped[ScrapedFromResponseModel.SCRAPYING_START_TIME] = start_time
            scraped[ScrapedFromResponseModel.SOURCE_OF_INFORMATION] = record[CrawlerResponseModel.SOURCE_OF_INFORMATION]

            # response_bodyをbs4で解析
            response_body: str = pickle.loads(record[CrawlerResponseModel.RESPONSE_BODY])
            soup: bs4 = bs4(response_body, 'lxml')
            scraped[ScrapedFromResponseModel.PATTERN] = {}

            # ドメイン別スクレイパー情報をDBより取得
            if not old_domain == record[CrawlerResponseModel.DOMAIN]:
                scraper_info_by_domain_data_list: list[ScraperInfoByDomainData] = []
                scraper_info_by_domain_data_list = scraper_info_by_domain.find_and_data_models_get(
                    filter={ScraperInfoByDomainConst.DOMAIN: record[CrawlerResponseModel.DOMAIN]})
                logger.info(
                    f'=== scrapying_run run  ドメイン別スクレイパー情報取得 (domain: {record[CrawlerResponseModel.DOMAIN]})')

            scraper_info_by_domain_data = scraper_info_by_domain_data_list[0]   # ドメイン単位で取得しているため常に１件
            for scraper, pattern_list in scraper_info_by_domain_data.scrape_item_get():
                if not scraper in scraper_mod:
                    scraper_mod[scraper] = import_module(
                        'prefect_lib.scraper.' + scraper)
                scraped_result, scraped_pattern = getattr(scraper_mod[scraper], 'scraper')(
                    soup=soup, scraper=scraper, scrape_parm=pattern_list,)
                scraped.update(scraped_result)
                scraped[ScrapedFromResponseModel.PATTERN].update(scraped_pattern)

            # データチェック
            warning_flg: bool = scraped_record_error_check(scraped)
            # if not warning_flg:
            #     scraped_from_response.insert_one(scraped)
            #     logger.info(
            #         f'=== scrapying_run run  処理対象url : {record[CrawlerResponseModel.URL]}')
            # タイトルしか無い記事も稀に存在する。有料会員にしか本文を見せていないニュースサイトもあるため、データが欠損していても保存はするよう見直し。
            scraped_from_response.insert_one(scraped)
            logger.info(
                f'=== scrapying_run run  処理対象url : {record[CrawlerResponseModel.URL]}')

            # 最後に今回処理を行ったdomainを保存
            old_domain = record[CrawlerResponseModel.DOMAIN]
