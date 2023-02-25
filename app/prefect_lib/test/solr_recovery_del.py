#!/usr/bin/python3
import pysolr
from datetime import datetime as dt
import sys, os
import pysolr
import logging
from logging import Logger

path = os.getcwd()
sys.path.append(path)
from shared.settings import TIMEZONE
from models.solr_news_clip_model import SolrNewsClip

logger: Logger = logging.getLogger()

def solr_del():

    solr = SolrNewsClip(logger)

    #publish_date :[2019-01-01T00:00:00Z TO 2019-01-31T23:59:59Z]
    #'2021-08-21T09:06:18+09:00 TO 2021-08-21T09:06:19+09:00'
    solr.delete_query(
        #'response_time:[2021-08-21T00:06:18Z TO 2021-08-21T00:06:19Z]',
        'response_time:[2021-08-20T00:06:18Z TO *]',
    )

    # solr.delete_id(delete_id_list=[
    #     "95c9a0ff-ef31-4e01-b061-9294daf6de35",
    # ])

    solr.commit()

if __name__ == '__main__':

    solr_del()