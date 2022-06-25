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
solr_news_clip = SolrNewsClip(logger)
results:Any = solr_news_clip.search_query(search_query, skip, limit)
print(results)
if results:
    recodes_count = results.raw_response['response']['numFound']

    print('=== ステータス',results.raw_response['responseHeader']['status'])
    print('=== カウント:',recodes_count)

    for recode in results.docs:
        print('=== docs:',results.docs)
        #print('=== url:',recode['url'])

    print('=== hits:',results.hits)
    print('=== debug:',results.debug)
    print('=== facets:',results.facets)
    print('=== stats:',results.stats)
    print('=== qtime:',results.qtime)
    print('=== nextCursorMark:',results.nextCursorMark)
    print('=== _next_page_query:',results._next_page_query)

    """ pysolr.Resultsの内部構造は以下の通り。
    ('__class__', <class 'pysolr.Results'>)
    {'raw_response':
        {'responseHeader': {'status': 0, 'QTime': 0, 'params': {'q': '*:*', 'start': '0', 'rows': '1', 'wt': 'json'}},
        'response': {'numFound': 7427, 'start': 0,
                    'docs': [{'mongo_id': ['〜'], 'url': '〜', 'title': '〜', 'article': '〜', 'issuer': ['〜'], 'update_count': 0, 'id': '〜', '_version_': 1638412764864053248, 'response_time': '2019-01-20T19:24:59.014Z', 'publish_date': '2018-04-01T00:00:00Z'}]}
        },
    'docs': [{'mongo_id': ['〜'], 'url': '〜', 'title': '〜', 'article': '〜', 'issuer': ['〜'], 'update_count': 0, 'id': '〜', '_version_': 1638412764864053248,'response_time': '2019-01-20T19:24:59.014Z', 'publish_date': '2018-04-01T00:00:00Z'}],
    'hits': 7427, 'debug': {}, 'highlighting': {}, 'facets': {}, 'spellcheck': {},
    'stats': {}, 'qtime': 0, 'grouped': {}, 'nextCursorMark': None, '_next_page_query': None
    }
    """
else:
    print('該当なし')

    #results_check = solr_news_clip.results_check(results)
    #results_article_cut = solr_news_clip.article_cut(results_check)
