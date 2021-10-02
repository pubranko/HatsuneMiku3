import os
import sys
import logging
import pickle
from typing import Any
from logging import Logger
from datetime import datetime
from pymongo.cursor import Cursor
from pymongo.command_cursor import CommandCursor
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
from models.asynchronous_report_model import AsynchronousReport
from common_lib.mail_send import mail_send

import pprint
from datetime import datetime, timezone, tzinfo, timedelta

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
    asynchronous_report_model = AsynchronousReport(mongo)

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
        log_filter: Any = {'$and': conditions}
    else:
        log_filter = None

    log_records: Cursor = crawler_logs.find(
        filter=log_filter,
        projection={'crawl_urls_list': 1, 'domain': 1}
    )

    # # ドメイン別の件数を取得してみた。これはクロール対象となった件数となる。
    # domain_aggregate:CommandCursor = crawler_logs.aggregate(
    #     aggregate_key='domain'
    # )

    ##############################
    # レスポンスの有無をチェック #
    ##############################
    conditions: list = []
    if domain:
        conditions.append({'domain': domain})
    if start_time_from:
        conditions.append(
            {'crawling_start_time': {'$gte': start_time_from}})
    if start_time_to:
        conditions.append(
            {'crawling_start_time': {'$lte': start_time_to}})

    response_sync_list: list = []   # crawler_logsとcrawler_responseで同期
    response_async_list: list = []  # crawler_logsとcrawler_responseで非同期
    asynchronous_domain_aggregate: dict = {}
    for log_record in log_records:

        asynchronous_domain_aggregate[log_record['domain']] = 0

        for crawl_urls_list in log_record['crawl_urls_list']:
            conditions.append({'url': crawl_urls_list['loc']})
            filter: Any = {'$and': conditions}
            response_records: Cursor = crawler_response.find(
                filter=filter,
                projection={'url': 1, 'response_time': 1, 'domain': 1},
            )

            if response_records.count() == 0:
                response_async_list.append(crawl_urls_list['loc'])
                # 非同期ドメイン集計カウントアップ
                asynchronous_domain_aggregate[log_record['domain']] += 1

            for response_record in response_records:
                response_sync_list.append({
                    'url': response_record['url'],
                    'response_time': timezone_recovery(response_record['response_time']),
                    'news_clip_master_register': response_record['news_clip_master_register'],
                })
            # 参照渡しなので最後に消さないと上述のresponse_recordsを参照した段階でエラーとなる
            conditions.pop(-1)

    # クロールミス分のurlがあれば、非同期レポートへ保存
    if len(response_async_list) > 0:
        asynchronous_report_model.insert_one({
            'record_type': 'news_crawl_async',
            'start_time': start_time,
            'crawler_logs_filter': pickle.dumps(log_filter),
            'response_non_list': response_async_list,
        })

        title: str = '【クローラー同期チェック：非同期発生】' + start_time.isoformat()

        msg: str = '以下のドメインがクローラーで対象となったにもかかわらず、レスポンス側にありません。\n'
        for item in asynchronous_domain_aggregate.items():
            if item[1] > 0:
                msg = msg + item[0] + ' : ' + str(item[1]) + ' 件\n'
        # mail_send(title, msg,)
        logger.error('=== 同期チェック結果(crawler -> response) : NG')
    else:
        logger.info('=== 同期チェック(crawler -> response)結果 : OK')

    ####################################
    # news_clip_masterの有無をチェック #
    ####################################
    mastar_conditions: list = []
    master_sync_list: list = []
    master_async_list: list = []

    for response_sync in response_sync_list:
        mastar_conditions = []
        mastar_conditions.append({'url': response_sync['url']})
        mastar_conditions.append({'response_time': response_sync['response_time']})

        filter: Any = {'$and': mastar_conditions}
        master_records: Cursor = news_clip_master.find(
            filter=filter,
            projection={'url': 1, 'response_time': 1}
        )
        if master_records.count() == 0:
            print('masterなし ', response_sync['url'])

            if not 'news_clip_master_register' in response_sync:
                print('スクレイピングでエラーとなった可能性大。マスタの登録処理まで進んでいない。')
                master_async_list.append(response_sync['url'])
            elif response_sync['news_clip_master_register'] == '登録内容に差異なしのため不要':
                print('内容に差異なしのため不要なデータ。問題なし。')


        for master_record in master_records:
            # レスポンスにあるのにマスターへの登録処理が行われていない。
            print('masterあり ', response_sync['url'])

            master_sync_list.append(
                (master_record['url'], timezone_recovery(master_record['response_time'])))
        mastar_conditions.pop(-1)

    #pprint.pprint(master_sync_list)
    #log_urls_list :list [url]
    # response_list :dict {'url':response_record['url'], 'response_time':timezone_recovery(response_record['response_time'])}
    # response_non_list
    # master_list:dict {'url':response_record['url'], 'response_time':timezone_recovery(response_record['response_time'])}
    # master_non_list
