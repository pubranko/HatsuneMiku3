import os
import sys
import logging
from typing import Any, Union
from logging import Logger, StreamHandler
from datetime import datetime
from importlib import import_module
from pymongo.cursor import Cursor
path = os.getcwd()
sys.path.append(path)
from models.crawler_response_model import CrawlerResponseModel
from prefect_lib.settings import TIMEZONE

logger:Logger = logging.getLogger('prefect.' +
                        sys._getframe().f_code.co_name)

def scrapying_deco(func):
    global logger

    def exec(kwargs):
        # 初期処理
        # 主処理
        func(kwargs)
        # 終了処理
        pass
    return exec


@scrapying_deco
def test1(kwargs):
    '''あとで'''
    global logger
    starting_time: datetime = kwargs['starting_time']
    crawler_response: CrawlerResponseModel = kwargs['crawler_response']
    domain: str = kwargs['domain']
    response_time_from: datetime = kwargs['response_time_from']
    response_time_to: datetime = kwargs['response_time_to']

    conditions :list = []
    if domain:
        conditions.append({'domain':domain})
    if response_time_from:
        conditions.append({'response_time':{'$gte':response_time_from}})
    if response_time_to:
        conditions.append({'response_time':{'$lte':response_time_to}})
    filter: dict = {'$and':conditions }
    logger.info('=== crawler_responseへのfilter: ' + str(filter))

    # スクレイピング対象件数を確認
    record_count = crawler_response.find(
        projection=None,
        filter=filter,
        sort=None
    ).count()
    logger.info('=== crawler_response スクレイピング対象件数 : ' + str(record_count))

    # 100件単位でスクレイピング処理を実施
    limit:int = 100
    skip_list = list(range(0,record_count, limit))

    for skip in skip_list:
        records: Cursor = crawler_response.find(
            projection=None,
            filter=filter,
            sort=None
        ).skip(skip).limit(limit)

        for record in records:
            module_name = record['domain'].replace('.','_')
            mod: Any = import_module('prefect_lib.scraper.' + module_name)
            scraped:dict = getattr(mod, 'exec')(record)

            scraped['domain':str] = record['domain']
            scraped['url':str] = record['url']
            rt:datetime = record['response_time']
            scraped['response_time':datetime] = rt.astimezone(TIMEZONE)

            print('=== 最後に戻り値:',scraped)


    '''
    1. ドメインごとのスクレイパーの呼び出し機能
    2. ドメインのドットをアンスコに変換した名前をモジュール名にする。
    3. メソッド名は、、、？
    4. メソッドの戻り値は、スクレイピングした各項目とする。dict形式で戻す。
    5. 最後に戻り値をmondoDBへ保存
    '''
