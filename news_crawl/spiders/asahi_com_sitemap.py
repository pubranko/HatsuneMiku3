from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider
from lxml.etree import _Element
from typing import Any


class AsahiComSitemapSpider(ExtensionsSitemapSpider):
    name = 'asahi_com_sitemap'
    allowed_domains = ['asahi.com']
    sitemap_urls: list = ['http://www.asahi.com/sitemap.xml']
    _domain_name: str = 'asahi_com'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    # イレギラーなサイトマップの場合、Trueにしてxml解析を各スパイダー用に切り替える。
    irregular_sitemap_parse_flg:bool = True

    @classmethod
    def irregular_sitemap_parse(cls, d: dict, el: _Element, name: Any):
        '''
        asahi.com専用のサイトマップ解析処理。
        lastmodがなくpublication_dateとなっているため、編集を行う。
        '''
        if name == 'link':
            if 'href' in el.attrib:
                d.setdefault('alternate', []).append(
                    el.get(key='href', default=None))
        elif name == 'loc':
            d[name] = el.text.strip() if el.text else ''
        elif name == 'lastmod':
            d[name] = el.text.strip() if el.text else ''
        elif name == 'news':
            publication_date: _Element = el.find('news:publication_date', namespaces={
                                                    'news': 'http://www.google.com/schemas/sitemap-news/0.9'})
            d['lastmod'] = publication_date.text
        else:
            d[name] = el.text.strip() if el.text else ''

        return d

