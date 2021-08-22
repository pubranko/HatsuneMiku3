import os
import sys
import logging
from logging import Logger
from typing import Any, Union
import pickle
from bs4 import BeautifulSoup as bs4
from bs4.element import ResultSet
from bs4.element import Tag
from datetime import datetime
from dateutil.parser import parse
from prefect_lib.settings import TIMEZONE

logger:Logger = logging.getLogger('prefect.scraper.asahi_com')

def exec(record:dict) -> dict:
    global logger
    #response_headers:str = pickle.loads(record['response_headers'])
    response_body:str = pickle.loads(record['response_body'])
    soup = bs4(response_body,'lxml')

    ### url
    url:str = record['url']
    logger.info('=== スクレイピングURL : ' + url)

    ### title
    temp:Any = soup.select_one('title')
    if temp:
        tag:Tag = temp
        title:str = tag.get_text()
    else:
        title = ''
        logger.error('=== スクレイピング：失敗(title)：URL : ' + url)

    ### article
    temp:Any = soup.select('.l-main > main > div > p,.l-main > main > div > h2')
    if temp:
        result_set:ResultSet = temp
        tag_list:list = [tag.get_text() for tag in result_set]
        article:str = '\n'.join(tag_list).strip()
    else:
        article:str = ''
        logger.error('=== スクレイピング：失敗(artcle)：URL : ' + url)

    ### publish_date
    temp:Any = soup.select_one('.l-main > main time')
    if temp:
        tag:Tag = temp
        publish_date = parse(tag['datetime']).astimezone(TIMEZONE)
    else:
        publish_date = None
        logger.error('=== スクレイピング：失敗(publish_date)：URL : ' + url)

    #発行者
    issuer = ['朝日新聞社','朝日新聞デジタル','朝日']

    return {'title':title,'article':article,'publish_date':publish_date,'issuer':issuer}