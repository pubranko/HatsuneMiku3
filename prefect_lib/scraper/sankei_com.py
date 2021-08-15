import os
import sys
import logging
from logging import Logger
from typing import Any, Union
import pickle
from bs4 import BeautifulSoup as bs4
from bs4.element import Tag
from datetime import datetime
from dateutil.parser import parse
from prefect_lib.settings import TIMEZONE

logger:Logger = logging.getLogger('prefect.' +
                        sys._getframe().f_code.co_name)

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
        title = temp.text
    else:
        title = ''
        logger.error('=== スクレイピング：失敗(title)：URL : ' + url)

    ### article
    temp:Any = soup.select('p.article-text')
    if temp:
        temp2:list = [x.text for x in temp]
        article:str = ''.join(temp2)
    else:
        article:str = ''
        logger.error('=== スクレイピング：失敗(artcle)：URL : ' + url)

    ### publish_date
    temp:Any = soup.select_one('div.article-meta-upper > time[datetime]')
    if temp:
        publish_date = parse(temp['datetime']).astimezone(TIMEZONE)
    else:
        publish_date = None
        logger.error('=== スクレイピング：失敗(publish_date)：URL : ' + url)

    #発行者
    issuer = '産経新聞社'

    #レスポンスタイム
    #rt:datetime = record['response_time']
    #response_time:datetime = rt.astimezone(TIMEZONE)
    #print('===これどうなっている？',type(response_time))
    #response_time:datetime = parse(record['response_time']).astimezone(TIMEZONE)
    #'response_time':response_time

    return {'title':title,'article':article,'publish_date':publish_date,'issuer':issuer}