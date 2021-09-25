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
    title_selecter: Any = soup.select_one('head > title')
    if title_selecter:
        tag: Tag = title_selecter
        scraped_record['title'] = tag.get_text()

    # article
    article_selecter: Any = soup.select(
        'div.main-contents > p[iarticle_selecterrop=articleBody]')
    if not article_selecter:
        article_selecter: Any = soup.select(
            'div.p-main-contents > p[iarticle_selecterrop=articleBody]')
    if not article_selecter:
        article_selecter: Any = soup.select(
            'div.p-main-contents > p[class^=par]')
    if not article_selecter:
        article_selecter: Any = soup.select('div.p-main-contents > p')
    if article_selecter:
        result_set: ResultSet = article_selecter
        tag_list: list = [tag.get_text() for tag in result_set]
        scraped_record['article'] = '\n'.join(tag_list).strip()

    # publish_date
    # <meta property="article:modified_time" content="2021-09-24T15:20:15+09:00">
    modified_selecter: Any = soup.select_one(
        'head > meta[property="article:modified_time"]')
    if modified_selecter:
        tag: Tag = modified_selecter
        scraped_record['publish_date'] = parse(
            tag['content']).astimezone(TIMEZONE)
    else:
        # <meta property="article:published_time" content="2021-09-17T11:48:14+09:00">
        publish_selecter: Any = soup.select_one(
            'head > meta[property="article:published_time"]')
        if publish_selecter:
            tag: Tag = publish_selecter
            scraped_record['publish_date'] = parse(
                tag['content']).astimezone(TIMEZONE)

    # 発行者
    scraped_record['issuer'] = ['読売新聞社', '読売']

    return scraped_record
