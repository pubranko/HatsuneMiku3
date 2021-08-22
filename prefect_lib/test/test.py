import os
import sys
path = os.getcwd()
sys.path.append(path)
from prefect_lib.test.SolrNewsClip import SolrNewsClip

#search_query = '*:*'
search_query = 'title:中国 and article:安倍'
#search_query = 'title:中国 or article:安倍'
page_num = 1
page_max_lines = 5

solr_news_clip = SolrNewsClip()
results = solr_news_clip.search_query(search_query, page_num, page_max_lines)

if results:
    for result in results:
        print(result)


#results_check = solr_news_clip.results_check(results)
#results_article_cut = solr_news_clip.article_cut(results_check)
