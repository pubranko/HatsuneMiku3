import os
import sys
import logging
from typing import Any
from logging import Logger
from datetime import datetime
from pymongo.cursor import Cursor
path = os.getcwd()
sys.path.append(path)
# from models.scraped_from_response_model import ScrapedFromResponse
# from models.news_clip_master_model import NewsClipMaster
from common_lib.timezone_recovery import timezone_recovery
from prefect_lib.common_module.scraped_record_error_check import scraped_record_error_check

from models.mongo_model import MongoModel
from models.crawler_logs_model import CrawlerLogsModel
from models.crawler_response_model import CrawlerResponseModel
from models.news_clip_master_model import NewsClipMaster
from models.solr_news_clip_model import SolrNewsClip

import pprint
from datetime import datetime,timezone,tzinfo,timedelta

logger: Logger = logging.getLogger('prefect.run.crawl_urls_sync_check_run')


def full_check(kwargs: dict):
    '''あとで'''
    print('full_check まできた')
    global logger
    start_time: datetime = kwargs['start_time']

    domain: str = kwargs['domain']
    start_time_from: datetime = kwargs['start_time_from']
    start_time_to: datetime = kwargs['start_time_to']

    mongo: MongoModel = kwargs['mongo']
    crawler_logs = CrawlerLogsModel(mongo)
    crawler_response = CrawlerResponseModel(mongo)
    news_clip_master = NewsClipMaster(mongo)
    solr_news_clip = SolrNewsClip(mongo)

    # スパイダーレポートより、クロール対象となったurlのリストを取得し一覧にする。
    conditions: list = []
    conditions.append({'record_type': 'spider_reports'})
    if domain:
        conditions.append({'domain': domain})
    if start_time_from:
        conditions.append(
            {'crawling_start_time': {'$gte': start_time_from}})
    if start_time_to:
        conditions.append(
            {'crawling_start_time': {'$lte': start_time_to}})

    if conditions:
        filter: Any = {'$and': conditions}
    else:
        filter = None

    log_records: Cursor = crawler_logs.find(
        filter=filter,
        projection={'crawl_urls_list': 1}
    )

    # ログからurlを抽出
    print(log_records.count())
    log_urls_list: list = []
    for log_record in log_records:
        log_record: dict
        log_urls_list.extend([crawl_urls_list['loc']
                               for crawl_urls_list in log_record['crawl_urls_list']])

    # レスポンスの有無をチェック
    conditions: list = []
    if domain:
        conditions.append({'domain': domain})
    if start_time_from:
        conditions.append(
            {'crawling_start_time': {'$gte': start_time_from}})
    if start_time_to:
        conditions.append(
            {'crawling_start_time': {'$lte': start_time_to}})

    response_list: list = []
    response_non_list:list = []
    for url in log_urls_list:
        conditions.append({'url': url})
        filter: Any = {'$and': conditions}
        response_records: Cursor = crawler_response.find(
            filter=filter,
            projection={'url': 1, 'response_time': 1}
        )

        if response_records.count() == 0:
            response_non_list.append(url)

        for response_record in response_records:
            response_list.append({'url':response_record['url'], 'response_time':timezone_recovery(response_record['response_time'])})
        conditions.pop(-1)  # 参照渡しなので最後に消さないと上述のresponse_recordsを参照した段階でエラーとなる

    pprint.pprint(response_list)

    # news_clip_masterの有無をチェック
    mastar_conditions: list = []
    master_list:list = []
    master_non_list:list = []

    print('====')

    for response in response_list:
        mastar_conditions = []
        mastar_conditions.append({'url': response['url']})
        mastar_conditions.append({'response_time': response['response_time']})

        # mastar_conditions = [
        #     {'url': 'https://jp.reuters.com/article/usa-fed-powell-idJPKBN2GO28N'},
        #     {'response_time': datetime(2021, 9, 29, 17, 54, 50, 113000, tzinfo=timezone(timedelta(seconds=32400), 'JST'))}
        # ]
        filter: Any = {'$and': mastar_conditions}
        print(filter)
        master_records: Cursor = news_clip_master.find(
            filter=filter,
            projection={'url': 1, 'response_time': 1}
        )
        if master_records.count() == 0:
            print('masterなし ',response['url'])
            master_non_list.append(response['url'])

        for master_record in master_records:
            print('masterあり ',response['url'])
            master_list.append((master_record['url'], timezone_recovery(master_record['response_time'])))
        mastar_conditions.pop(-1)

    pprint.pprint(master_list)
    #log_urls_list :list [url]
    #response_list :dict {'url':response_record['url'], 'response_time':timezone_recovery(response_record['response_time'])}
    #response_non_list
    #master_list:dict {'url':response_record['url'], 'response_time':timezone_recovery(response_record['response_time'])}
    #master_non_list

    '''
    1. response_non_list
        (1)urlをDBに貯める
            ・非同期レポート、asynchronous_report
            ・record_type : news_crawl_async
        (2)メールで非同期が発生していることを通知する。（ドメイン別件数まで）
        (3)別のflowをにて手動起動し、非同期レポートからurlを抽出する。重複を除去した一覧を作成する。
        (4)scrapy_crawling_taskを実行する。
            引数には抽出したurlをkwargs['urls']へ直接渡す。
    2. master_non_list
        (1)スクレイピングでエラーとなっているケースと、news_clip_masterへの保存の際、
            データ重複で除去されているケースがある。
            現在後者の判定ができない。
        (2)news_clip_masterへの登録時に重複skipとなった情報をログに出力しておく。
            ・record_type = duplicate_skip
            ・collection = news_clip_master
            ・url、response_time,crawling_start_time、id、、、どうする？urlだけでいける？
        (3)当runで上記(2)を読み込み、(1)の後者の判定を行い同期チェックを行う。
        (4)url,crawling_start_timeをDBに貯める
            ・非同期レポート、asynchronous_report
            ・record_type : news_clip_master_async
        (5)メールで非同期が発生していることを通知する。（ドメイン別件数まで）
        (6) 別のflowをにて手動起動し、非同期レポートからurl,crawling_start_timeを抽出し、scrapying_taskを実行する。
            引数には抽出したurlをkwargs['urls']へ直接渡す。
            response_timeかidでレコードの特定して抽出する処理を実装するのが良さそう。



    '''