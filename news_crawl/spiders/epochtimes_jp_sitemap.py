from typing import Pattern
from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider
from news_crawl.items import NewsCrawlItem
from datetime import datetime
import pickle
from scrapy.http.response.html import HtmlResponse
import re


class EpochtimesJpSitemapSpider(ExtensionsSitemapSpider):
    name = 'epochtimes_jp_sitemap'
    allowed_domains = ['epochtimes.jp']
    #start_urls = ['http://epochtimes.jp/']
    sitemap_urls: list = [
        'https://www.epochtimes.jp/sitemap/sitemap-latest.xml',  # 最新
    ]
    _domain_name: str = 'epochtimes_jp'        # 各種処理で使用するドメイン名の一元管理
    spider_version: float = 1.0
    custom_settings: dict = {
        'DEPTH_LIMIT': 2,
        'DEPTH_STATS_VERBOSE': True,
        #'LOG_FILE' : 'logs/test_log('+ name +').txt',
    }

    # 大紀元のsitemapに記載されているurlは、リダイレクト前のurl。
    # リダイレクト後のurlへ変換してクロールするようカスタマイズ。
    _custom_url_flg:bool = True

    def _custom_url(self,_entry:dict) -> str:
        '''(オーバーライド)
        sitemapのurlをカスタマイズする。\n
        ・変換前例 → https://www.epochtimes.jp/2021/05/73403.html \n
        ・変換後例 → https://www.epochtimes.jp/p/2021/05/73403.html
        '''
        pattern:Pattern = re.compile('https://www.epochtimes.jp/')
        return pattern.sub('https://www.epochtimes.jp/p/',_entry['loc'])
