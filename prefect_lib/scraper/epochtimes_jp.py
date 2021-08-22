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

logger: Logger = logging.getLogger('prefect.scraper.epochtimes_jp')


def exec(record: dict) -> dict:
    global logger
    #response_headers:str = pickle.loads(record['response_headers'])
    response_body: str = pickle.loads(record['response_body'])
    soup = bs4(response_body, 'lxml')

    # url
    url: str = record['url']
    logger.info('=== スクレイピングURL : ' + url)

    # title
    temp: Any = soup.select_one('title')
    if temp:
        tag: Tag = temp
        title:str = tag.text
    else:
        title = ''
        logger.error('=== スクレイピング：失敗(title)：URL : ' + url)

    # article
    temp: Any = soup.select('article .page_content')
    if temp:
        result_set:ResultSet = temp
        tag: Tag = result_set[0]
        article: str = tag.get_text().strip()
    else:
        article: str = ''
        logger.error('=== スクレイピング：失敗(artcle)：URL : ' + url)

    # publish_date
    temp: Any = soup.select_one('.page_datetime.col-sm-12.col-md-12')
    if temp:
        tag: Tag = temp
        publish_date = datetime.strptime(tag.get_text().replace('\n','').strip(), '%Y年%m月%d日 %H時%M分').astimezone(TIMEZONE)
    else:
        publish_date = None
        logger.error('=== スクレイピング：失敗(publish_date)：URL : ' + url)

    # 発行者
    issuer = ['大紀元時報日本', 'エポックタイムズジャパン', 'Epoch Times Japan', '大紀元', ]

    return {'title': title, 'article': article, 'publish_date': publish_date, 'issuer': issuer}
