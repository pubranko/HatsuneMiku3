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
from common_lib.timezone_recovery import timezone_recovery

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
    title_selecter: Any = soup.select_one('head > title')
    if title_selecter:
        tag: Tag = title_selecter
        scraped_record['title'] = tag.get_text()

    # article
    article_selecter: Any = soup.select('article .page_content')
    if not article_selecter:
        article_selecter: Any = soup.select(
            '.middle_part > .text')
    if article_selecter:
        result_set: ResultSet = article_selecter
        tag: Tag = result_set[0]
        scraped_record['article'] = tag.get_text().strip()

    # publish_date
    publish_selecter: Any = soup.select_one(
        'head > meta[name="pubdate"]')

    publish_selecter: Any = soup.select_one('.page_datetime.col-sm-12.col-md-12')
    if publish_selecter:
        tag: Tag = publish_selecter
        scraped_record['publish_date'] = datetime.strptime(tag.get_text().replace(
            '\n', '').strip(), '%Y年%m月%d日 %H時%M分').astimezone(TIMEZONE)
    else:
        scraped_record['publish_date'] = timezone_recovery(record['source_of_information']['lastmod'])

    # 発行者
    scraped_record['issuer'] = ['大紀元時報日本',
                                'エポックタイムズジャパン', 'Epoch Times Japan', '大紀元', ]

    return scraped_record
