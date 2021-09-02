import os
import sys
from typing import Union
from datetime import datetime, timedelta
from dateutil import parser
path = os.getcwd()
sys.path.append(path)
from common_lib.timezone_recovery import timezone_recovery


class CrawlingContinuedSkipCheck(object):
    ''''''
    crawl_point_save: dict = {}
    latest_lastmod: datetime
    latest_url: str = ''
    kwargs_save: dict = {}

    def __init__(self, crawl_point: dict, sitemap_url: str, kwargs: dict) -> None:
        '''
        '''
        self.kwargs_save = kwargs

        if 'continued' in kwargs:
            self.crawl_point_save = crawl_point
            self.latest_lastmod = timezone_recovery(
                crawl_point[sitemap_url]['latest_lastmod'])
            self.latest_url = crawl_point[sitemap_url]['latest_url']

    def skip_check(self, lastmod: datetime, url: str) -> bool:
        '''
        '''
        crwal_flg: bool = False
        if 'continued' in self.kwargs_save:
            if lastmod < self.latest_lastmod:
                crwal_flg = True
            elif lastmod == self.latest_lastmod and url == self.latest_url:
                crwal_flg = True

        return crwal_flg
