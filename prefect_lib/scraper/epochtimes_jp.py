import os
import sys
import logging
from logging import Logger
from typing import Any, Union
import pickle
from bs4 import BeautifulSoup as bs4
from bs4.element import Tag
from bs4.element import ResultSet
from datetime import datetime
from dateutil.parser import parse
from prefect_lib.settings import TIMEZONE

file_name = os.path.splitext(os.path.basename(__file__))[0]
logger: Logger = logging.getLogger('prefect.scraper.' + file_name)


def exec(record: dict, kwargs: dict) -> dict:
    global logger
    #response_headers:str = pickle.loads(record['response_headers'])
    response_body: str = pickle.loads(record['response_body'])
    soup = bs4(response_body, 'lxml')
    scraped_record: dict = {}

    # url
    url: str = record['url']
    scraped_record['url'] = url
    logger.info('=== スクレイピングURL : ' + url)

    # title
    temp: Any = soup.select_one('title')
    if temp:
        tag: Tag = temp
        scraped_record['title'] = tag.get_text()

    # article
    temp: Any = soup.select('article .page_content')
    if temp:
        result_set: ResultSet = temp
        tag: Tag = result_set[0]
        scraped_record['article'] = tag.get_text().strip()

    # publish_date
    temp: Any = soup.select_one('.page_datetime.col-sm-12.col-md-12')
    if temp:
        tag: Tag = temp
        scraped_record['publish_date'] = datetime.strptime(tag.get_text().replace(
            '\n', '').strip(), '%Y年%m月%d日 %H時%M分').astimezone(TIMEZONE)

    # 発行者
    scraped_record['issuer'] = ['大紀元時報日本',
                                'エポックタイムズジャパン', 'Epoch Times Japan', '大紀元', ]

    return scraped_record
