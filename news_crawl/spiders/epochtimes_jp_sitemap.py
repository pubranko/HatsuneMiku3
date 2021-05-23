from typing import Pattern
from news_crawl.spiders.extensions_sitemap import ExtensionsSitemapSpider
from news_crawl.items import NewsCrawlItem
from datetime import datetime
import pickle
from scrapy.http.response.html import HtmlResponse
import re


class EpochtimesJpSpider(ExtensionsSitemapSpider):
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
    }

    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _sitemap_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _sitemap_next_crawl_info: dict = {name: {}, }
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

    def parse(self, response: HtmlResponse):
        '''
        取得したレスポンスよりDBへ書き込み
        '''
        _info = self.name + ':' + str(self.spider_version) + ' / ' \
            + 'extensions_sitemap:' + str(self._extensions_sitemap_version)

        yield NewsCrawlItem(
            url=response.url,
            response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
            response_headers=pickle.dumps(response.headers),
            response_body=pickle.dumps(response.body),
            spider_version_info=_info
        )
