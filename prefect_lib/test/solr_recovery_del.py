#!/usr/bin/python3
import pysolr
from datetime import datetime as dt
import sys, os
import pysolr
import logging
from logging import Logger

path = os.getcwd()
sys.path.append(path)
from models.solr_news_clip_model import SolrNewsClip
from prefect_lib.settings import TIMEZONE

logger: Logger = logging.getLogger()

def solr_del():

    solr = SolrNewsClip(logger)

    #publish_date :[2019-01-01T00:00:00Z TO 2019-01-31T23:59:59Z]
    #'2021-08-21T09:06:18+09:00 TO 2021-08-21T09:06:19+09:00'
    solr.delete_query(
        #'response_time:[2021-08-21T00:06:18Z TO 2021-08-21T00:06:19Z]',
        'response_time:[2021-08-20T00:06:18Z TO *]',
    )

    # solr = pysolr.Solr(
    #     os.environ['SOLR_URL'] + os.environ['SOLR_CORE'],
    #     timeout=30,
    #     verify='',
    #     # verify=SolrEnv.VERIFY,     #solrの公開鍵を指定する場合、ここにファイルパスを入れる。
    #     auth=(os.environ['SOLR_WRITE_USER'],
    #             os.environ['SOLR_WRITE_PASS']),
    #     #always_commit=True,
    # )

    #solr.delete(id=["e234a380-e9c9-44f0-8b50-d0181c8a057e"])
    # solr.delete(id=[
    #     "94dcbcce-fe9a-4cab-b134-450e1bb04526",
    #     "2f312294-1ea4-45b9-a925-dcc45cdeed5f",
    # ])

    solr.commit()

if __name__ == '__main__':

    solr_del()