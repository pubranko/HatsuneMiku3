from dateutil import parser

from news_crawl.spiders.extensions_sitemap import ExtensionsSitemapSpider
from datetime import datetime, timedelta


class SankeiComSitemapSpider(ExtensionsSitemapSpider):
    name: str = 'sankei_com_sitemap'
    allowed_domains: list = ['sankei.com']
    sitemap_urls: list = ['https://www.sankei.com/sitemap.xml', ]
    _domain_name: str = 'sankei_com'        # 各種処理で使用するドメイン名の一元管理
    spider_version: float = 1.0

    # sitemap_urlsに複数のサイトマップを指定した場合、その数だけsitemap_filterが可動する。その際、どのサイトマップか判別できるように処理中のサイトマップと連動するカウント。
    _sitemap_urls_count: int = 0
    # crawler_controllerコレクションへ書き込むレコードのdomain以降のレイアウト雛形。※最上位のKeyのdomainはサイトの特性にかかわらず固定とするため。
    _sitemap_next_crawl_info: dict = {name: {}, }

    # def __init__(self, *args, **kwargs):
    #     super().__init__(*args, **kwargs)
    #     print('=== ',__class__,' の__init__終了')

    # def sitemap_filter(self, entries):
    #     print('=== SankeiComSitemapSpider のsaitemap_filter start')
