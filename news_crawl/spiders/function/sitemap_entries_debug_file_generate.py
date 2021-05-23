from typing import Any
import os
from scrapy.spiders import Spider
from scrapy.utils.sitemap import Sitemap

def sitemap_entries_debug_file_generate(name:str,kwargs_save:Any,entries:Sitemap,sitemap_url: str) -> None:
    ''' (拡張メソッド)
    デバックモードが指定された場合、entriesと元となったsitemapのurlをdebug_entries.txtへ出力する。
    '''
    if 'debug' in kwargs_save:         # sitemap調査用。debugモードの場合のみ。
        path: str = os.path.join(
            'news_crawl', 'debug', 'debug_entries(' + name + ').txt')
        with open(path, 'a') as _:
            for _entry in entries:
                _.write(sitemap_url + ',' + _entry['lastmod'] + ',' + _entry['loc'] + '\n')
