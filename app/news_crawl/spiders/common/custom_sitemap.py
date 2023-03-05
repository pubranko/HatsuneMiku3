from __future__ import annotations  # ExtensionsSitemapSpiderの循環参照を回避するため
from typing import Any, TYPE_CHECKING
import lxml.etree
from lxml.etree import XMLParser, _Element
from scrapy.http import Response

if TYPE_CHECKING:
    from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider


class CustomSitemap:
    """
    以下のSitemapを元にカスタマイズしたクラス。
    from scrapy.utils.sitemap import Sitemap
    """
    _root: Any
    spider: ExtensionsSitemapSpider

    def __init__(self, xmltext, response: Response, spider: ExtensionsSitemapSpider):
        xmlp: XMLParser = lxml.etree.XMLParser(
            recover=True, remove_comments=True, resolve_entities=False)
        self._root = lxml.etree.fromstring(xmltext, parser=xmlp)
        rt = self._root.tag
        self.type = self._root.tag.split('}', 1)[1] if '}' in rt else rt
        self.spider = spider

    def __iter__(self):
        self._root
        for elem in self._root.getchildren():
            d = {}
            for el in elem.getchildren():
                el: _Element
                tag = el.tag
                name = tag.split('}', 1)[1] if '}' in tag else tag

                # イレギラーなsitemapの解析には、各スパイダーのirregular_sitemap_parseを使用するようカスタマイズ
                if self.spider.sitemap_type == self.spider.SITEMAP_TYPE__IRREGULAR:
                    d = self.spider.irregular_sitemap_parse(d, el, name)
                elif self.spider.sitemap_type == self.spider.SITEMAP_TYPE__GOOGLE_NEWS_SITEMAP:
                    '''
                    googleのニュースサイトマップタイプ解析処理。
                    lastmodがなく<news>タグ内の<publication_date>となっているため、カスタマイズを行う。
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
                        if publication_date == None:
                            publication_date: _Element = el.find('news:publication_date', namespaces={
                                                                    'news': 'https://www.google.com/schemas/sitemap-news/0.9'})

                        d['lastmod'] = publication_date.text
                    else:
                        d[name] = el.text.strip() if el.text else ''
                else:
                    if name == 'link':
                        if 'href' in el.attrib:
                            d.setdefault('alternate', []).append(
                                el.get(key='href', default=None))
                    else:
                        d[name] = el.text.strip() if el.text else ''

            if 'loc' in d:
                yield d


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

