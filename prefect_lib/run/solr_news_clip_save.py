import os
import sys
import logging
from typing import Any, Union
from logging import Logger
from datetime import datetime
from dateutil import tz
from dateutil.parser import parse
from pymongo.cursor import Cursor
import pysolr
path = os.getcwd()
sys.path.append(path)
from models.news_clip_master_model import NewsClipMaster
from models.solr_news_clip_model import SolrNewsClip
from prefect_lib.settings import TIMEZONE

logger: Logger = logging.getLogger('prefect.run.news_clip_master_save')


def check_and_save(kwargs):
    '''あとで'''
    global logger
    print('=== check_and_saveで確認',kwargs)
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

    solr_news_clip = SolrNewsClip(logger)
    # 100件単位で処理を実施
    limit: int = 100
    skip_list = list(range(0, record_count, limit))

    for skip in skip_list:
        master_records: Cursor = news_clip_master.find(
            filter=filter,
        ).skip(skip).limit(limit)

        for master_record in master_records:

            _ = 'url:' + solr_news_clip.escape_convert(master_record['url'])
            query:list = [_]
            solr_results: Any = solr_news_clip.search_query(query)

            solr_recodes_count = solr_results.raw_response['response']['numFound']

            new_record:dict = {
                "url": master_record['url'],
                "title": master_record['title'],
                "article": master_record['article'],
                "response_time": master_record['response_time'],
                "publish_date": master_record['publish_date'],
                "issuer": master_record['issuer'],
            }
            if solr_recodes_count:
                for solr_recode in solr_results.docs:
                    # 取得したレコードのpublish_dateのタイムゾーンを修正(MongoDB?のバグのらしい)
                    UTC = tz.gettz("UTC")
                    master_publish_date: datetime = master_record['publish_date']
                    master_publish_date = master_publish_date.replace(tzinfo=UTC)
                    master_publish_date = master_publish_date.astimezone(TIMEZONE)

                    solr_publish_date:datetime = parse(solr_recode['publish_date']).astimezone(TIMEZONE)

                    # 重複チェック
                    if master_record['title'] == solr_recode['title']  and \
                        master_record['article'] == solr_recode['article'] and \
                        master_publish_date == solr_publish_date:

                        logger.info('=== solrに登録済みのためスキップ: ' + str(new_record['url']))
                    else:
                        solr_news_clip.add(new_record)
                        logger.warning('=== solrの登録情報と差異あり（追加）: ' + str(new_record['url']))
            else:
                pass
                solr_news_clip.add(new_record)
                logger.info('=== solrへ新規追加: ' + str(new_record['url']))

        solr_news_clip.commit()
