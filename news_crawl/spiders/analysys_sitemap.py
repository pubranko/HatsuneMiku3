import sys

from scrapy.spiders.sitemap import SitemapSpider
from news_crawl.spiders.extensions_sitemap import ExtensionsSitemapSpider
from datetime import timedelta


class AnalysysSitemapSpider(ExtensionsSitemapSpider):
    '''
    サイトマップ解析用ツール。解析したいサイトマップURLを指定して実行するだけ。
    '''
    name:str = 'analysys_sitemap'
    allowed_domains = []
    sitemap_urls: list = ['https://jp.reuters.com/sitemap_20210517-20210518.xml']
    #_domain_name: str = 'jp_reuters_com_crawl'        # 各種処理で使用するドメイン名の一元管理
    #spider_version: float = 1.0
    custom_settings: dict = {
        'ROBOTSTXT_OBEY': False,
        'DEPTH_LIMIT': 1,
        'DEPTH_STATS_VERBOSE': True
    }

    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)

    def sitemap_filter(self, entries):
        '''
        extensions_sitemapのsitemap_filterで各サイトをクロールしないようにオーバーライド。
        ただしサイトマップ本体の保存機能は使用する。
        '''
        print('=== sitemap_filter ===')
        self.sitemap_filter_common_prosses(
            entries, self.sitemap_urls[self._sitemap_urls_count])

        for _entry in entries:
            '''
            引数に絞り込み指定がある場合、その条件を満たす場合のみ対象とする。
            '''
            _crwal_flg: bool = False
            if _crwal_flg: yield _entry


    def parse(self, response):
        '''
        extensions_sitemapのparseで保存しないようにオーバーライド
        '''
        pass

    def closed(self, spider):
        '''
        extensions_sitemapのclosedで保存しないようにオーバーライド
        '''
        self.mongo.close()
