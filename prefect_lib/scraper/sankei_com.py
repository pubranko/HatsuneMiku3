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

import time


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
    #body > div#fusion-app > div.grid div.grid > section > article.article-wrapper > div.article-body > p.article-text
    article_selecter: Any = soup.select('body  article.article-wrapper > div.article-body > p.article-text')
    if not article_selecter:
        print('＠＠＠ ＠＠＠ 産経　article　type2')
        article_selecter: Any = soup.select(
            '.article-body > .article-text')
    if article_selecter:
        result_set: ResultSet = article_selecter
        tag_list: list = [tag.get_text() for tag in result_set]
        scraped_record['article'] = '\n'.join(tag_list).strip()
    p5 = time.perf_counter()

    # publish_date
    modified_selecter: Any = soup.select_one('meta[name="article:modified_time"]')
    if modified_selecter:
        tag: Tag = modified_selecter
        scraped_record['publish_date'] = parse(
            tag['content']).astimezone(TIMEZONE)
    else:
        #publish_selecter: Any = soup.select_one('div.article-meta-upper > time[datetime]')
        #publish_selecter: Any = soup.select_one('div.article-datetime > time[datetime]')
        publish_selecter: Any = soup.select_one('meta[name="article:published_time"]')
        if publish_selecter:
            tag: Tag = publish_selecter
            scraped_record['publish_date'] = parse(
                tag['content']).astimezone(TIMEZONE)

    p6 = time.perf_counter()
    # 発行者
    scraped_record['issuer'] = ['産経新聞社', '産経']

    return scraped_record
