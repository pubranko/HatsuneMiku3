import os
import sys
import logging
from typing import Any, Tuple
from logging import Logger
from datetime import datetime
from dateutil import tz
from pymongo.cursor import Cursor
import pysolr
path = os.getcwd()
sys.path.append(path)
from shared.timezone_recovery import timezone_recovery
from BrownieAtelierNotice.mail_send import mail_send
from BrownieAtelierMongo.models.mongo_model import MongoModel
from BrownieAtelierMongo.models.crawler_logs_model import CrawlerLogsModel
from BrownieAtelierMongo.models.crawler_response_model import CrawlerResponseModel
from BrownieAtelierMongo.models.news_clip_master_model import NewsClipMasterModel
from BrownieAtelierMongo.models.asynchronous_report_model import AsynchronousReportModel
from models.solr_news_clip_model import SolrNewsClip

logger: Logger = logging.getLogger('prefect.run.crawl_urls_sync_check_run')


def check(kwargs: dict):
    '''
    ①crawl対象のurlとcrawler_responseの同期チェック
    ②crawler_responseとnews_clip_masterの同期チェック
    ③news_clip_masterとsolrのnews_clipの同期チェック
    '''
    def crawler_response_sync_check() -> Tuple[list, list, dict]:
        '''①crawl対象のurlとcrawler_responseの同期チェック'''

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
        response_async_domain_aggregate: dict = {}
        for log_record in log_records:

            # domain別の集計エリアを初期設定
            response_async_domain_aggregate[log_record['domain']] = 0

            # crawl_urls_listからをクロール対象となったurlを抽出
            loc_crawl_urls: list = []
            for temp in log_record['crawl_urls_list']:
                loc_crawl_urls.extend([item['loc'] for item in temp['items']])

            # スパイダーレポートよりクロール対象となったurlを順に読み込み、crawler_responseに登録されていることを確認する。
            for crawl_url in loc_crawl_urls:
                conditions.append({'url': crawl_url})
                master_filter: Any = {'$and': conditions}
                response_records: Cursor = crawler_response.find(
                    filter=master_filter,
                    projection={'url': 1, 'response_time': 1, 'domain': 1},
                )

                # crawler_response側に存在しないクロール対象urlがある場合
                if crawler_response.count(filter=master_filter) == 0:
                    response_async_list.append(crawl_url)
                    # 非同期ドメイン集計カウントアップ
                    response_async_domain_aggregate[log_record['domain']] += 1

                # クロール対象とcrawler_responseで同期している場合、同期リストへ保存
                # ※定期観測では1件しか存在しないないはずだが、start_time_from〜toで一定の範囲の
                # 同期チェックを行った場合、複数件発生する可能性がある。
                for response_record in response_records:
                    _ = {
                        'url': response_record['url'],
                        'response_time': timezone_recovery(response_record['response_time']),
                        'domain': response_record['domain'],
                    }
                    if 'news_clip_master_register' in response_record:
                        _['news_clip_master_register'] = response_record['news_clip_master_register']
                    response_sync_list.append(_)

                # 参照渡しなので最後に消さないと上述のresponse_recordsを参照した段階でエラーとなる
                conditions.pop(-1)

        # クロールミス分のurlがあれば、非同期レポートへ保存
        if len(response_async_list) > 0:
            asynchronous_report_model.insert_one({
                'record_type': 'news_crawl_async',
                'start_time': start_time,
                'parameter': {
                    'domain': domain,
                    'start_time_from': start_time_from,
                    'start_time_to': start_time_to,
                },
                'async_list': response_async_list,
            })
            counter = f'エラー({len(response_async_list)})/正常({len(response_sync_list)})'
            logger.error(
                f'=== 同期チェック結果(crawler -> response) : NG({counter})')
        else:
            logger.info(
                f'=== 同期チェック(crawler -> response)結果 : OK(件数 : {len(response_sync_list)})')

        return response_sync_list, response_async_list, response_async_domain_aggregate

    def news_clip_master_sync_check() -> Tuple[list, list, dict]:
        '''②crawler_responseとnews_clip_masterの同期チェック'''
        mastar_conditions: list = []
        master_sync_list: list = []
        master_async_list: list = []
        master_async_domain_aggregate: dict = {}
        master_filter: Any = ''

        # crawler_responseで同期しているリストを順に読み込み、news_clip_masterに登録されているか確認する。
        for response_sync in response_sync_list:
            mastar_conditions = []
            mastar_conditions.append({'url': response_sync['url']})
            mastar_conditions.append(
                {'response_time': response_sync['response_time']})
            master_filter = {'$and': mastar_conditions}
            master_records: Cursor = news_clip_master.find(
                filter=master_filter,
                projection={'url': 1, 'response_time': 1, 'domain': 1}
            )

            # news_clip_master側に存在しないcrawler_responseがある場合
            if news_clip_master.count(filter=master_filter) == 0:
                if not 'news_clip_master_register' in response_sync:
                    master_async_list.append(response_sync['url'])

                    # 非同期ドメイン集計カウントアップ
                    if response_sync['domain'] in master_async_domain_aggregate:
                        master_async_domain_aggregate[response_sync['domain']] += 1
                    else:
                        master_async_domain_aggregate[response_sync['domain']] = 1

                elif response_sync['news_clip_master_register'] == '登録内容に差異なしのため不要':
                    pass  # 内容に差異なしのため不要なデータ。問題なし

            # crawler_responseとnews_clip_masterで同期している場合、同期リストへ保存
            # ※通常1件しかないはずだが、障害によりリカバリした場合などは複数件存在する可能性がある。
            for master_record in master_records:
                # レスポンスにあるのにマスターへの登録処理が行われていない。
                _ = {
                    'url': master_record['url'],
                    # 'response_time': timezone_recovery(master_record['response_time']),
                    'response_time': master_record['response_time'],
                    'domain': master_record['domain'],
                }
                master_sync_list.append(_)

        # スクレイピングミス分のurlがあれば、非同期レポートへ保存
        if len(master_async_list) > 0:
            asynchronous_report_model.insert_one({
                'record_type': 'news_clip_master_async',
                'start_time': start_time,
                'parameter': {
                    'domain': domain,
                    'start_time_from': start_time_from,
                    'start_time_to': start_time_to,
                },
                'async_list': master_async_list,
            })
            counter = f'エラー({len(master_async_list)})/正常({len(master_sync_list)})'
            logger.error(f'=== 同期チェック結果(response -> master) : NG({counter})')
        else:
            logger.info(
                f'=== 同期チェック結果(response -> master) : OK(件数 : {len(master_sync_list)})')
        return master_sync_list, master_async_list, master_async_domain_aggregate

    def solr_news_clip_sync_check() -> Tuple[list, list, dict]:
        '''③news_clip_masterとsolrのnews_clipの同期チェック'''
        solr_conditions: list = []
        solr_sync_list: list = []
        solr_async_list: list = []
        solr_async_domain_aggregate: dict = {}
        solr_filter: Any = ''

        UTC = tz.gettz("UTC")

        # news_clip_masterで同期しているリストを順に読み込み、solr_news_clipに登録されているか確認する。
        for master_sync in master_sync_list:
            solr_query: list = [
                'url:', solr_news_clip.escape_convert(master_sync['url']),
                ' and ',
                'response_time:', solr_news_clip.escape_convert(
                    master_sync['response_time'].isoformat() + 'Z'),
            ]
            field: list = [
                'url',
                'response_time',
                # 'domain',
            ]

            solr_records_temp: Any = solr_news_clip.search_query(
                search_query=solr_query, field=field)

            # news_clip_master側に存在しないcrawler_responseがある場合
            if solr_records_temp:
                solr_records: pysolr.Results = solr_records_temp
                # crawler_responseとnews_clip_masterで同期している場合、同期リストへ保存
                # ※通常1件しかないはずだが、障害によりリカバリした場合などは複数件存在する可能性がある。
                for solr_record in solr_records:
                    # レスポンスにあるのにマスターへの登録処理が行われていない。
                    solr_sync_list.append(
                        (solr_record['url'], solr_record['response_time']))
            else:
                solr_async_list.append(master_sync['url'])

                # 非同期ドメイン集計カウントアップ
                if master_sync['domain'] in solr_async_domain_aggregate:
                    solr_async_domain_aggregate[master_sync['domain']] += 1
                else:
                    solr_async_domain_aggregate[master_sync['domain']] = 1

        # スクレイピングミス分のurlがあれば、非同期レポートへ保存
        if len(solr_async_list) > 0:
            asynchronous_report_model.insert_one({
                'record_type': 'solr_news_clip_async',
                'start_time': start_time,
                'parameter': {
                    'domain': domain,
                    'start_time_from': start_time_from,
                    'start_time_to': start_time_to,
                },
                'async_list': solr_async_list,
            })
            counter = f'エラー({len(solr_async_list)})/正常({len(solr_sync_list)})'
            logger.error(f'=== 同期チェック結果(master -> solr) : NG({counter})')
        else:
            logger.info(
                '=== 同期チェック結果(master -> solr) : OK(件数 : {len(solr_sync_list)})')

        return solr_sync_list, solr_async_list, solr_async_domain_aggregate

    #################################################################
    global logger
    start_time: datetime = kwargs['start_time']

    domain: str = kwargs['domain']
    start_time_from: datetime = kwargs['start_time_from']
    start_time_to: datetime = kwargs['start_time_to']

    mongo: MongoModel = kwargs['mongo']
    crawler_logs = CrawlerLogsModel(mongo)
    crawler_response = CrawlerResponseModel(mongo)
    news_clip_master = NewsClipMasterModel(mongo)
    solr_news_clip = SolrNewsClip(mongo)
    asynchronous_report_model = AsynchronousReportModel(mongo)

    # crawl結果とcrawler_responseが同期しているかチェックする。
    response_sync_list, response_async_list, response_async_domain_aggregate = crawler_response_sync_check()
    # crawler_responseとnews_clip_masterが同期しているかチェックする。
    master_sync_list, master_async_list, master_async_domain_aggregate = news_clip_master_sync_check()

    # 当分の間solrとの同期チェックは中止。solr側の本格開発が始まってから開放予定。
    #solr_sync_list, solr_async_list, solr_async_domain_aggregate = solr_news_clip_sync_check()

    title: str = '【クローラー同期チェック：非同期発生】' + start_time.isoformat()
    message: str = ''
    # クロールミス分のurlがあれば
    if len(response_async_list) > 0:
        # メール通知用メッセージ追記
        message = message + '以下のドメインでクローラーで対象となったにもかかわらず、crawler_responseに登録されていないケースがあります。\n'
        for item in response_async_domain_aggregate.items():
            if item[1] > 0:
                message = message + item[0] + ' : ' + str(item[1]) + ' 件\n'

    # スクレイピングミス分のurlがあれば
    if len(master_async_list) > 0:
        # メール通知用メッセージ追記
        message = message + \
            '以下のドメインでcrawler_responseにあるにもかかわらず、news_clip_master側に登録されていないケースがあります。\n'
        for item in master_async_domain_aggregate.items():
            if item[1] > 0:
                message = message + item[0] + ' : ' + str(item[1]) + ' 件\n'

    # 当分の間solrとの同期チェックは中止。solr側の本格開発が始まってから開放予定。
    # # solrへの送信ミス分のurlがあれば
    # if len(solr_async_list) > 0:
    #     # メール通知用メッセージ追記
    #     message = message + '以下のドメインでnews_clip_masterにあるにもかかわらず、solr_news_clip側に登録されていないケースがあります。\n'
    #     for item in solr_async_domain_aggregate.items():
    #         if item[1] > 0:
    #             message = message + item[0] + ' : ' + str(item[1]) + ' 件\n'

    # エラーがあった場合エラー通知を行う。
    if not message == '':
        mail_send(title, message, logger)
