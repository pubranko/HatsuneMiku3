from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider
from lxml.etree import _Element
from typing import Any


class AsahiComSitemapSpider(ExtensionsSitemapSpider):
    name = 'asahi_com_sitemap'
    allowed_domains = ['asahi.com']
    sitemap_urls: list = ['http://www.asahi.com/sitemap.xml']
    _domain_name: str = 'asahi_com'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    sitemap_type = ExtensionsSitemapSpider.SITEMAP_TYPE__GOOGLE_NEWS_SITEMAP    #googleのニュースサイトマップ用にカスタマイズしたタイプ

    known_pagination_css_selectors:list[str] = [
        '.entryPageNav  a[href]',
    ]

    # selenium_mode: bool = True
    # sitemap_rules = [(r'.*', 'selenium_parse')]