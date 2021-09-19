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
    type1: Any = soup.select_one('title')
    if type1:
        tag: Tag = type1
        scraped_record['title'] = tag.get_text()

    # article
    type1: Any = soup.select('div.main-contents > p[itemprop=articleBody]')
    type2: Any = soup.select('div.p-main-contents > p[itemprop=articleBody]')
    if type1:
        result_set: ResultSet = type1
        tag_list: list = [tag.get_text() for tag in result_set]
        scraped_record['article'] = '\n'.join(tag_list).strip()
    elif type2:
        result_set: ResultSet = type1
        tag_list: list = [tag.get_text() for tag in result_set]
        scraped_record['article'] = '\n'.join(tag_list).strip()

    # publish_date
    type1: Any = soup.select_one('div.c-article-header-date > time[datetime]')
    if type1:
        tag: Tag = type1
        scraped_record['publish_date'] = parse(
            tag['datetime']).astimezone(TIMEZONE)

    # 発行者
    scraped_record['issuer'] = ['読売新聞社', '読売']

    return scraped_record
