from __future__ import annotations
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
from models.mongo_model import MongoModel
from models.crawler_response_model import CrawlerResponseModel
from models.scraped_from_response_model import ScrapedFromResponseModel
from models.scraper_info_by_domain_model import ScraperInfoByDomainModel
from models.controller_model import ControllerModel
from common_lib.timezone_recovery import timezone_recovery
from prefect_lib.scraper.article_scraper import scraper as artcle_scraper
from prefect_lib.scraper.publish_date_scraper import scraper as publish_date_scraper
from prefect_lib.scraper.title_scraper import scraper as title_scraper

from collections.abc import Iterable
path = os.getcwd()
sys.path.append(path)
from prefect_lib.settings import DEBUG_FILE_DIR


logger: Logger = logging.getLogger('prefect.run.scrapying_deco')


start_time: datetime = datetime.now()
mongo: MongoModel = MongoModel()
crawler_response: CrawlerResponseModel = CrawlerResponseModel(mongo)
scraped_from_response: ScrapedFromResponseModel = ScrapedFromResponseModel(
    mongo)
scraper_by_domain: ScraperInfoByDomainModel = ScraperInfoByDomainModel(mongo)
controller: ControllerModel = ControllerModel(mongo)

conditions: list = []
urls: list[str] = [
    #'https://www.asahi.com/articles/ASQ5P42N4Q5PUCVL005.html',
    'https://www.sankei.com/article/20220521-FYEUS72WDRMTRLILLWPA5DHRAA/',
]
scrape_parm = [{
    "pattern": 1,
    "css_selecter": "head > meta[name=\"pubdate\"]",
}]

conditions.append({'url': {'$in': urls}})
if conditions:
    filter: Any = {'$and': conditions}
else:
    filter = None
logger.info(f'=== crawler_responseへのfilter: {str(filter)}')

# スクレイピング対象件数を確認
records: Cursor = crawler_response.find(
    projection=None,
    filter=filter,
    sort=[('domain', ASCENDING), ('response_time', ASCENDING)],
)
for record in records:
    # 各サイト共通の項目を設定
    # response_bodyをbs4で解析
    response_body: str = pickle.loads(record['response_body'])
    #print('\n\n\n',response_body)
    soup: bs4 = bs4(response_body, 'lxml')
    #page_source = soup.select_one('html')
    #print('\n\n\n',soup.select_one('html'))

    path: str = os.path.join(
        DEBUG_FILE_DIR, f'response_data.html')
    with open(path, 'w') as file:
        file.write(str(soup.select_one('html')))

    #print('\n\n\n',response_body)

    scrape_parm = sorted(scrape_parm, key=lambda d: d['pattern'], reverse=True)
    print('\n\n=== scrape_parm ===', scrape_parm)

    result = publish_date_scraper(
        soup=soup,
        scraper='publish_scraper',
        scrape_parm=scrape_parm,
    )
    print('\n\n=== result ===', result)