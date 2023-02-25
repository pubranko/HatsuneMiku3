import os
import sys
import logging
from typing import Any
from logging import Logger
from datetime import datetime
from pymongo import ASCENDING
from pymongo.cursor import Cursor
path = os.getcwd()
sys.path.append(path)
from BrownieAtelierMongo.models.mongo_model import MongoModel
from BrownieAtelierMongo.models.news_clip_master_model import NewsClipMasterModel
from shared.timezone_recovery import timezone_recovery
from models.solr_news_clip_model import SolrNewsClip

logger: Logger = logging.getLogger('prefect.run.news_clip_master_save')


def check_and_save(kwargs: dict):
    '''あとで'''
    global logger
    start_time: datetime = kwargs['start_time']
    mongo: MongoModel = kwargs['mongo']
    news_clip_master: NewsClipMasterModel = NewsClipMasterModel(mongo)

    domain: str = kwargs['domain']
    scraped_save_start_time_from: datetime = kwargs['scraped_save_start_time_from']
    scraped_save_start_time_to: datetime = kwargs['scraped_save_start_time_to']

    conditions: list = []
    if domain:
        conditions.append({'domain': domain})
    if scraped_save_start_time_from:
        conditions.append(
            {'scraped_save_start_time': {'$gte': scraped_save_start_time_from}})
    if scraped_save_start_time_to:
        conditions.append(
            {'scraped_save_start_time': {'$lte': scraped_save_start_time_to}})

    if conditions:
        filter: Any = {'$and': conditions}
    else:
        filter = None

    logger.info('=== news_clip_master へのfilter: ' + str(filter))

    # 対象件数を確認
    record_count = news_clip_master.count(filter=filter)
    logger.info('=== solr の news_clip への登録チェック対象件数 : ' + str(record_count))

    solr_news_clip = SolrNewsClip(logger)
    # 100件単位で処理を実施
    limit: int = 100
    skip_list = list(range(0, record_count, limit))

    for skip in skip_list:
        master_records: Cursor = news_clip_master.find(
            filter=filter,
            sort=[('response_time', ASCENDING)],
        ).skip(skip).limit(limit)

        for master_record in master_records:
            _ = 'url:' + solr_news_clip.escape_convert(master_record['url'])
            query: list = [_]
            solr_results: Any = solr_news_clip.search_query(query)
            solr_recodes_count = solr_results.raw_response['response']['numFound']

            new_record: dict = {
                "url": master_record['url'],
                "title": master_record['title'],
                "article": master_record['article'],
                "response_time": timezone_recovery(master_record['response_time']),
                "publish_date": timezone_recovery(master_record['publish_date']),
                "issuer": master_record['issuer'],
            }
            if solr_recodes_count:
                for solr_recode in solr_results.docs:
                    # 重複チェック
                    if master_record['title'] == solr_recode['title'] and \
                            master_record['article'] == solr_recode['article']:  # and \
                        logger.info('=== solrに登録済みのためスキップ: ' +
                                    str(new_record['url']))
                    else:
                        solr_news_clip.add(new_record)
                        logger.warning(
                            '=== solrの登録情報と差異あり（追加）: ' + str(new_record['url']))
            else:
                solr_news_clip.add(new_record)
                logger.info('=== solrへ新規追加: ' + str(new_record['url']))

        solr_news_clip.commit()
