from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider
from lxml.etree import _Element
from typing import Any


class YomiuriCoJpSitemapSpider(ExtensionsSitemapSpider):
    name: str = 'yomiuri_co_jp_sitemap'
    allowed_domains: list = ['yomiuri.co.jp']
    sitemap_urls: list = ['https://www.yomiuri.co.jp/sitemap.xml']
    _domain_name: str = 'yomiuri_co_jp'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    # https://www.yomiuri.co.jp/sitemap-pt-post-2021-09-04.xml
    #sitemap_follow = ['/sitemap-pt-post-']
    # https://www.yomiuri.co.jp/sitemap-news-latest.xml
    sitemap_follow = ['/sitemap-news-latest']

    sitemap_type = 'google_news_sitemap'    #googleのニュースサイトマップ用にカスタマイズしたタイプ

    # @classmethod
    # def irregular_sitemap_parse(cls, d: dict, el: _Element, name: Any):
    #     '''
    #     asahi.com専用のサイトマップ解析処理。
    #     lastmodがなくpublication_dateとなっているため、編集を行う。
    #     '''
    #     if name == 'link':
    #         if 'href' in el.attrib:
    #             d.setdefault('alternate', []).append(
    #                 el.get(key='href', default=None))
    #     elif name == 'loc':
    #         d[name] = el.text.strip() if el.text else ''
    #     elif name == 'lastmod':
    #         d[name] = el.text.strip() if el.text else ''
    #     elif name == 'news':
    #         publication_date: _Element = el.find('news:publication_date', namespaces={
    #                                                 'news': 'http://www.google.com/schemas/sitemap-news/0.9'})
    #         d['lastmod'] = publication_date.text
    #     else:
    #         d[name] = el.text.strip() if el.text else ''

    #     return d

