from datetime import datetime
from typing import Any
import pandas as pd
from copy import deepcopy
import itertools
from typing import Union

class ScrapyCrawlingKwargsInput:
    lastmod_period_minutes:Union[list,None] = None
    pages:Union[list,None] = None
    continued:Union[str,None] = None
    direct_crawl_urls:Union[list,None] = None
    debug:Union[str,None] = None
    crawl_point_non_update:Union[str,None] = None
    url_pattern:Union[str,None] = None

    def __init__(self, spider_kwargs):
        if 'lastmod_period_minutes' in spider_kwargs:
            self.lastmod_period_minutes= spider_kwargs['lastmod_period_minutes']
        if 'pages' in spider_kwargs:
            self.pages= spider_kwargs['pages']
        if 'continued' in spider_kwargs:
            self.continued= spider_kwargs['continued']
        if 'direct_crawl_urls' in spider_kwargs:
            self.direct_crawl_urls= spider_kwargs['direct_crawl_urls']
        if 'debug' in spider_kwargs:
            self.debug= spider_kwargs['debug']
        if 'crawl_point_non_update' in spider_kwargs:
            self.crawl_point_non_update= spider_kwargs['crawl_point_non_update']
        if 'url_pattern' in spider_kwargs:
            self.url_pattern= spider_kwargs['url_pattern']

    def spider_kwargs_correction(self) -> dict[str,Any]:
        '''スパイダー用引数補正'''
        return {
            'lastmod_period_minutes': self.lastmod_period_minutes,
            'pages': self.pages,
            'continued': self.continued,
            'direct_crawl_urls': self.direct_crawl_urls,
            'debug': self.debug,
            'crawl_point_non_update': self.crawl_point_non_update,
            'url_pattern': self.url_pattern,
        }