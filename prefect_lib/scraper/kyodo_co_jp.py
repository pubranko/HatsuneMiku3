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
    article_selecter: Any = soup.select(
        'div.area-content > div.entry-content p')
    if article_selecter:
        result_set: ResultSet = article_selecter
        tag_list: list = [tag.get_text() for tag in result_set]
        scraped_record['article'] = '\n'.join(tag_list).strip()

    # publish_date
    modified_selecter: Any = soup.select_one(
        'head > meta[name="iso-8601-modified-date"]')
    if modified_selecter:
        tag: Tag = modified_selecter
        scraped_record['publish_date'] = parse(
            tag['content']).astimezone(TIMEZONE)
    else:
        # publish_date
        # <meta name="iso-8601-publish-date" content="2021-09-24T09:00:58+00:00">
        publish_selecter: Any = soup.select_one(
            'head > meta[name="iso-8601-publish-date"]')
        if publish_selecter:
            tag: Tag = publish_selecter
            scraped_record['publish_date'] = parse(
                tag['content']).astimezone(TIMEZONE)

    # 共同のpublish_dateは日付のみ。時間部分がないため、sitemap側のlastmodで代用する。
    #scraped_record['publish_date'] = timezone_recovery(record['sitemap_data']['lastmod'])

    # 発行者
    scraped_record['issuer'] = ['共同通信社', '共同']

    return scraped_record
