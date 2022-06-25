import os
import sys
from typing import Any
import pysolr
import logging
from logging import Logger

path = os.getcwd()
sys.path.append(path)
from models.solr_news_clip_model import SolrNewsClip

logger: Logger = logging.getLogger()

#search_query = ['*:*']
#search_query = ['title:中国 AND article:安倍']
#search_query = ['title:中国 OR article:安倍']
#search_query = ['(title:中国 OR article:安倍)']
search_query = ['title:中国',' AND ','article:安倍']
skip = 0
limit = 5

# 環境変数からsolrの情報を取得するように変更
solr = pysolr.Solr(
    os.environ['SOLR_URL'] + os.environ['SOLR_CORE'],
    timeout=30,
    verify='',
    # verify=SolrEnv.VERIFY,     #solrの公開鍵を指定する場合、ここにファイルパスを入れる。
    auth=(os.environ['SOLR_ADMIN_USER'],
            os.environ['SOLR_ADMIN_PASS']),
    always_commit=True,
)

print(solr.ping())
print(solr.url)
print(solr.use_qt_param)
print(solr.decoder)
results = solr.search('*:*')
print(len(results))
# for result in results:
#     #print(result)
#     result
results = solr.search('url:*', **{
    'rows': limit,
    'start': skip,
    # ソートのやり方。 desc降順 asc昇順。 %20は空白に置き換えること。'response_time desc,',
    #'sort': ''.join(sort_list),
    # 取得したフィールドを限定したい場合
    #'fl': ','.join(field),
    # ファセット、取得したいフィールド
    #'facet': facet,
    #'facet_field': ','.join(facet_field),
})
