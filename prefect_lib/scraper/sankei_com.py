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

logger:Logger = logging.getLogger('prefect.scraper.sankei_com')

def exec(record:dict) -> dict:
    global logger
    #response_headers:str = pickle.loads(record['response_headers'])
    response_body:str = pickle.loads(record['response_body'])
    soup = bs4(response_body,'lxml')
    scraped_record:dict = {}

    ### url
    url:str = record['url']
    scraped_record['url'] = url
    logger.info('=== スクレイピングURL : ' + url)

    ### title
    temp:Any = soup.select_one('title')
    if temp:
        tag:Tag = temp
        scraped_record['title'] = tag.get_text()
    else:
        logger.error('=== スクレイピング：失敗(title)：URL : ' + url)

    ### article
    temp:Any = soup.select('p.article-text')
    if temp:
        result_set:ResultSet = temp
        tag_list:list = [tag.get_text() for tag in result_set]
        scraped_record['article'] = '\n'.join(tag_list).strip()
    else:
        logger.error('=== スクレイピング：失敗(artcle)：URL : ' + url)

    ### publish_date
    temp:Any = soup.select_one('div.article-meta-upper > time[datetime]')
    if temp:
        tag:Tag = temp
        scraped_record['publish_date'] = parse(tag['datetime']).astimezone(TIMEZONE)
    else:
        logger.error('=== スクレイピング：失敗(publish_date)：URL : ' + url)

    #発行者
    scraped_record['issuer'] = ['産経新聞社','産経']

    return scraped_record