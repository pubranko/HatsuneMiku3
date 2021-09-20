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

file_name = os.path.splitext(os.path.basename(__file__))[0]
logger: Logger = logging.getLogger('prefect.scraper.' + file_name)

def exec(record:dict, kwargs:dict) -> dict:
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

    ### article
    temp:Any = soup.select('.l-main > main > div > p,.l-main > main > div > h2')
    if temp:
        result_set:ResultSet = temp
        tag_list:list = [tag.get_text() for tag in result_set]
        scraped_record['article'] = '\n'.join(tag_list).strip()

    ### publish_date
    temp:Any = soup.select_one('.l-main > main time')
    if temp:
        tag:Tag = temp
        scraped_record['publish_date'] = parse(tag['datetime']).astimezone(TIMEZONE)

    #発行者
    scraped_record['issuer'] = ['朝日新聞社','朝日新聞デジタル','朝日']

    return scraped_record