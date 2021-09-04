import os
import sys
from typing import Union,Any
from datetime import datetime, timedelta
from dateutil import parser
path = os.getcwd()
sys.path.append(path)
from common_lib.timezone_recovery import timezone_recovery


class CrawlingContinuedSkipCheck(object):
    ''''''
    crawl_point_save: dict = {}
    latest_lastmod: Any = None
    #latest_url: str = ''
    kwargs_save: dict = {}

    #def __init__(self, crawl_point: dict, sitemap_url: str, kwargs: dict) -> None:
    def __init__(self, crawl_point: dict, kwargs: dict) -> None:
        '''
        '''
        self.kwargs_save = kwargs

        if 'continued' in kwargs:
            self.crawl_point_save = crawl_point
            self.latest_lastmod = timezone_recovery(
                crawl_point['latest_lastmod'])
            # self.latest_lastmod = timezone_recovery(
            #     crawl_point[sitemap_url]['latest_lastmod'])
            #self.latest_url = crawl_point[sitemap_url]['latest_url']

    def skip_check(self, lastmod: datetime) -> bool:
        '''
        '''
        crwal_flg: bool = False
        if 'continued' in self.kwargs_save:
            if lastmod < self.latest_lastmod:
                crwal_flg = True
            # elif lastmod == self.latest_lastmod and url == self.latest_url:
            #     crwal_flg = True

        return crwal_flg

    def max_lastmod_dicision(self,in_the_page_max_lastmod:datetime) -> datetime:
        '''
        サイトマップ内の最大更新時間を引数として受け取る。
        受け取った引数とドメイン内の最大更新時間の記録と比較し、
        新しい最大更新時間を決定する。
        '''
        if self.latest_lastmod:
            if in_the_page_max_lastmod > self.latest_lastmod:
                return in_the_page_max_lastmod
            else:
                return self.latest_lastmod
        else:
            self.latest_lastmod = in_the_page_max_lastmod
            return in_the_page_max_lastmod
