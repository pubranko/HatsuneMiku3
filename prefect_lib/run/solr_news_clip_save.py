import os
import sys
import logging
from typing import Any, Union
from logging import Logger
from datetime import datetime
from dateutil import tz
from pymongo.cursor import Cursor
path = os.getcwd()
sys.path.append(path)
from models.news_clip_master_model import NewsClipMaster
from models.solr_news_clip_model import SolrNewsClip
from prefect_lib.settings import TIMEZONE

logger: Logger = logging.getLogger('prefect.run.news_clip_master_save')


def check_and_save(kwargs):
    '''あとで'''
    global logger
    starting_time: datetime = kwargs['starting_time']
    news_clip_master: NewsClipMaster = kwargs['news_clip_master']

    domain: str = kwargs['domain']
    scraped_save_starting_time_from: datetime = kwargs['scraped_save_starting_time_from']
    scraped_save_starting_time_to: datetime = kwargs['scraped_save_starting_time_to']

    conditions: list = []
    if domain:
        conditions.append({'domain': domain})
    if scraped_save_starting_time_from:
        conditions.append(
            {'scraped_save_starting_time': {'$gte': scraped_save_starting_time_from}})
    if scraped_save_starting_time_to:
        conditions.append(
            {'scraped_save_starting_time': {'$lte': scraped_save_starting_time_to}})

    if conditions:
        filter: Any = {'$and': conditions}
    else:
        filter = None

    logger.info('=== news_clip_master へのfilter: ' + str(filter))

    # 対象件数を確認
    record_count = news_clip_master.find(
        filter=filter,
    ).count()
    logger.info('=== solr の news_clip への登録チェック対象件数 : ' + str(record_count))

    # 100件単位で処理を実施
    limit: int = 100
    skip_list = list(range(0, record_count, limit))

    for skip in skip_list:
        master_records: Cursor = news_clip_master.find(
            filter=filter,
        ).skip(skip).limit(limit)

        for master_record in master_records:

            solr_news_clip = SolrNewsClip()
            query:list = ['url:' + master_record['url']]
            solr_results: Any = solr_news_clip.search_query(
                query, skip=0, limit=0)

            solr_recodes_count = solr_results.raw_response['response']['numFound']

            if solr_recodes_count:
                pass
                #url登録なし。solrへ追加する
            else:
                for solr_recode in solr_results.docs:
                    # 重複チェック
                    if master_record['title'] == solr_recode['title']  and \
                        master_record['article'] == solr_recode['article'] and \
                        master_record['publish_date'] == solr_recode['publish_date']:
                        pass
                        #既登録。追加不要。
                    else:
                        pass
                        #登録内容の更新あり。solrへ追加する。


'''
            # 取得したレコードのscraped_save_starting_timeのタイムゾーンを修正(MongoDB? PyMongo?のバグのらしい)
            UTC = tz.gettz("UTC")
            dt: datetime = master_record['scraped_save_starting_time']
            dt = dt.replace(tzinfo=UTC)
            dt = dt.astimezone(TIMEZONE)

            # 重複するレコードがなければ保存する。
            st: str = dt.isoformat()
            if news_clip_duplicate_count == 0:
                master_record['scraped_save_starting_time'] = starting_time
                news_clip_master.insert_one(master_record)
                logger.info('=== news_clip_master への登録 : ' +
                            st + ' : ' + master_record['url'])
            else:
                logger.info('=== news_clip_master への登録スキップ : ' +
                            st + ' : ' + master_record['url'])
'''