from __future__ import annotations
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
    scraping_type_by_item: dict = {}

    ### url ###
    url: str = record['url']
    scraped_record['url'] = url
    logger.info('=== スクレイピングURL : ' + url)

    # title
    title_selecter = soup.select_one('head > title')
    if type(title_selecter) is Tag:
        scraping_type_by_item['title'] = 'type_1'
        scraped_record['title'] = title_selecter.get_text()

    ### article ###
    # body > div#fusion-app > div.grid div.grid > section > article.article-wrapper > div.article-body > p.article-text

    article_scraping_parm:list =[
        {'type':'parse_type_2',
         'css_selecter':'.article-body > .article-text'},
        {'type':'parse_type_1',
         'css_selecter':'body  article.article-wrapper > div.article-body > p.article-text'},
    ]
    article_selecter = soup.select(
        'body  article.article-wrapper > div.article-body > p.article-text')
    if article_selecter:
        scraping_type_by_item['article'] = 'type_1'

    if not article_selecter:
        scraping_type_by_item['article'] = 'type_2'
        article_selecter = soup.select('.article-body > .article-text')

    if type(article_selecter) is ResultSet:
        tag_list: list[str] = [tag.get_text() for tag in article_selecter]
        scraped_record['article'] = '\n'.join(tag_list).strip()

    ### publish_date ###
    scraping_type_by_item['publish_date'] = 'type_1'
    publish_selecter = soup.select_one('meta[name="article:modified_time"]')
    if not publish_selecter:
        scraping_type_by_item['publish_date'] = 'type_2'
        publish_selecter = soup.select_one(
            'meta[name="article:published_time"]')
    if type(publish_selecter) is Tag:
        scraped_record['publish_date'] = parse(
            str(publish_selecter['content'])).astimezone(TIMEZONE)

    ### 発行者 ###
    scraped_record['issuer'] = ['産経新聞社', '産経']

    ### 各スクレイピングのバージョンを統計のため記録 ###
    scraped_record['scraping_type_by_item'] = scraping_type_by_item

    return scraped_record
