from __future__ import annotations
from tabnanny import check
from pydantic import UrlUserInfoError
from news_crawl.spiders.extensions_class.extensions_sitemap import ExtensionsSitemapSpider

from datetime import datetime
import pickle
import scrapy

import urllib.parse
from urllib.parse import ParseResult
from scrapy.http import Response,TextResponse
from news_crawl.items import NewsCrawlItem
import re



class YomiuriCoJpSitemapSpider(ExtensionsSitemapSpider):
    name: str = 'yomiuri_co_jp_sitemap'
    allowed_domains: list = ['yomiuri.co.jp']
    sitemap_urls: list = ['https://www.yomiuri.co.jp/sitemap.xml']
    #sitemap_urls: list = ['https://www.yomiuri.co.jp/sitemap-news-past-1.xml']
    _domain_name: str = 'yomiuri_co_jp'        # 各種処理で使用するドメイン名の一元管理
    _spider_version: float = 1.0

    # https://www.yomiuri.co.jp/sitemap-pt-post-2021-09-04.xml
    #sitemap_follow = ['/sitemap-pt-post-']
    # https://www.yomiuri.co.jp/sitemap-news-latest.xml
    ###一時的にOFF　########################sitemap_follow = ['/sitemap-news-latest']

    sitemap_type = 'google_news_sitemap'    #googleのニュースサイトマップ用にカスタマイズしたタイプ

    known_pagination_css_selectors:list[str] = [
        #'.p-article-wp-pager a[href]::attr(href)',
    ]

    # def parse(self, response: TextResponse):
    #     '''
    #     取得したレスポンスよりDBへ書き込み
    #     '''
    #     ### ページ内の対象urlを抽出
    #     for link in response.css('[href]::attr(href)').getall():
    #         # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
    #         link_url: str = urllib.parse.unquote(response.urljoin(link))
    #         # リンクのurlがsitemapで対象としたurlの別ページだった場合、クロール対象のurlへ追加する。
    #         if self.pagination_check(response,link_url):
    #             yield scrapy.Request(response.urljoin(link_url), callback=self.parse)

    #         #汎用的なページネーションチェック機能の実装途中
    #         #・sitemap側でurlを貯める機能を追加する。
    #         #・その追加されたurl_listの部分一致するリンクの場合、次ページとみなすような仕組みにしてみようと思う。

    #     # ### ページ内の対象urlを抽出
    #     # for css_selecter in self.pagination_list:
    #     #     for link in response.css(css_selecter).getall():
    #     #         # 相対パスの場合絶対パスへ変換。また%エスケープされたものはUTF-8へ変換
    #     #         url: str = urllib.parse.unquote(response.urljoin(link))
    #     #         check_url:ParseResult = urllib.parse.urlparse(url)
    #     #         yield scrapy.Request(response.urljoin(url), callback=self.parse)
    #     ################################
    #     _info = self.name + ':' + str(self._spider_version) + ' / ' \
    #         + 'extensions_sitemap:' + str(self._extensions_sitemap_version)

    #     source_of_information: dict = {}
    #     for record in self.crawl_urls_list:
    #         record: dict
    #         if response.url == record['loc']:
    #             source_of_information['source_url'] = record['source_url']
    #             source_of_information['lastmod'] = record['lastmod']

    #     yield NewsCrawlItem(
    #         domain=self.allowed_domains[0],
    #         url=response.url,
    #         response_time=datetime.now().astimezone(self.settings['TIMEZONE']),
    #         response_headers=pickle.dumps(response.headers),
    #         response_body=pickle.dumps(response.body),
    #         spider_version_info=_info,
    #         crawling_start_time=self._crawling_start_time,
    #         source_of_information=source_of_information,
    #     )
